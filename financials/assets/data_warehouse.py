import sys
import pandas as pd
import numpy as np
from dagster import (#AssetIn,  # SourceAsset,; Output,
                    #  DailyPartitionsDefinition, 
                     ExperimentalWarning,
                     MetadataValue, #MultiPartitionKey,
                    #  MultiPartitionsDefinition, Optional, PartitionKeyRange,
                    #  PartitionMapping, PartitionsDefinition,
                    #  StaticPartitionsDefinition, 
                    asset,# op,
                    #  LastPartitionMapping
                    AssetIn
                     )
from icecream import ic

from financials.financials_config import * #pylint: disable=wildcard-import, unused-wildcard-import

from financials.resources.helpers import (
    standardise_types
)


if not sys.warnoptions:
    import warnings
    warnings.filterwarnings("ignore", category=ExperimentalWarning)

from financials.assets.data_lake import (
    market_day_multipartition,
    # v3_market_day_multipartition
)


@asset(
    compute_kind='python',
    partitions_def=market_day_multipartition,
    group_name='data_warehouse',
    code_version="1",
    io_manager_key = 'data_warehouse_io_manager',
    ins={
        "block_numbers_by_day": AssetIn(key_prefix="financials_data_lake"),
    }
)
def blocks_by_day(context, block_numbers_by_day) -> pd.DataFrame:
    """
    Table with the closest block number to the daily partition boundary (0000 UTC) for a chain

    Uses the upstream per-market asset and only returns a per-chain result
    Uses the market config parameter block_table_master to determine which market writes this table

    Args:
        context: dagster context object
        block_numbers_by_day: the output of block_numbers_by_day

    Returns:
        A dataframe with the block details for the master chain only

    """
    date, market = context.partition_key.split("|")
    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")
    
    is_master = CONFIG_MARKETS[market]['block_table_master']
    
    if is_master:
        return_val = block_numbers_by_day[['block_day','block_time','block_height','end_block','chain']]
    else:
        return_val = pd.DataFrame()

    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )
    return return_val


@asset(
    compute_kind='python',
    partitions_def=market_day_multipartition,
    group_name='data_warehouse',
    code_version="1",
    io_manager_key = 'data_warehouse_io_manager',
    ins={
        "collector_atoken_balances_by_day": AssetIn(key_prefix="financials_data_lake"),
        "collector_atoken_transfers_by_day": AssetIn(key_prefix="financials_data_lake"),
        "v3_accrued_fees_by_day": AssetIn(key_prefix="financials_data_lake"),
        "v3_minted_to_treasury_by_day": AssetIn(key_prefix="financials_data_lake"),
        "internal_external_addresses": AssetIn(key_prefix="financials_data_lake"),
    }
)
def atoken_measures_by_day(
            context,
            collector_atoken_balances_by_day,
            collector_atoken_transfers_by_day,
            v3_accrued_fees_by_day,
            v3_minted_to_treasury_by_day,
            internal_external_addresses
            ) -> pd.DataFrame:
    """
    Joins all measures relevant to an atoken into one table

    Args:
        context: dagster context object
        collector_atoken_balances_by_day: the output of collector_atoken_balances_by_day
        collector_atoken_transfers_by_day: the output of collector_atoken_transfers_by_day
        v3_accrued_fees_by_day: the output of v3_accrued_fees_by_day
        v3_minted_to_treasury_by_day: the output of v3_minted_to_treasury_by_day
        internal_external_addresses: the output of internal_external_addresses

    Returns:
        A dataframe with a row for each atoken on the day, and the output of all the measures
        joined to the atoken metadata.  Non existing measures are set to 0

    """
    date, market = context.partition_key.split("|")
    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")
    chain = CONFIG_MARKETS[market]['chain']

    return_val = collector_atoken_balances_by_day
    if not return_val.empty:
        if not v3_accrued_fees_by_day.empty:
            v3_accrued_fees_by_day = v3_accrued_fees_by_day[['market','atoken','atoken_symbol','block_height','block_day','accrued_fees']].copy()
            v3_accrued_fees_by_day.rename(columns={'atoken':'token','atoken_symbol':'symbol'}, inplace=True)
            return_val = return_val.merge(v3_accrued_fees_by_day,
                                        how='left',
                                        #   left_on=['market','token','symbol','block_height','block_day'],
                                        #   right_on=['market','atoken','atoken_symbol','block_height','block_day']
                                        )
        else:
            return_val['accrued_fees'] = float(0)

        if not collector_atoken_transfers_by_day.empty:
            # transfers need to be classified as internal/external and aggregated to day level
            transfers = collector_atoken_transfers_by_day.copy()
            transfers.columns = transfers.columns.str.replace('transfers_','')

            transfers = transfers[[
                    'market',
                    'collector',
                    'transfer_type',
                    'from_address',
                    'to_address',
                    'contract_address',
                    'contract_symbol',
                    'block_day',
                    'amount_transferred'
            ]]
            transfers.rename(columns={'contract_address':'token','contract_symbol':'symbol'}, inplace=True)

            # classify transfers as internal or external
            transfers_in = transfers.loc[transfers['transfer_type']=='IN']
            transfers_out = transfers.loc[transfers['transfer_type']=='OUT']
            transfer_class = internal_external_addresses.loc[internal_external_addresses['chain']==chain]
            transfer_class = transfer_class[['contract_address','internal_external']].copy()

            if not transfers_in.empty:
                    transfer_class_in = transfer_class.rename(columns={'contract_address':'from_address'})
                    transfers_in = transfers_in.merge(transfer_class_in, how='left')
            
            if not transfers_out.empty:
                    transfer_class_out = transfer_class.rename(columns={'contract_address':'to_address'})
                    transfers_out = transfers_out.merge(transfer_class_out, how='left')

            transfers = pd.concat([transfers_in, transfers_out])
            transfers.internal_external.fillna('aave_external', inplace=True)
            
            # create columns for internal and external transfers
            transfers['tokens_in_external'] = np.where((transfers['internal_external']=='aave_external') & (transfers['transfer_type'] == 'IN'), transfers['amount_transferred'], float(0))
            transfers['tokens_in_internal'] = np.where((transfers['internal_external']=='aave_internal') & (transfers['transfer_type'] == 'IN'), transfers['amount_transferred'], float(0))
            transfers['tokens_out_external'] = np.where((transfers['internal_external']=='aave_external') & (transfers['transfer_type'] == 'OUT'), transfers['amount_transferred'], float(0))
            transfers['tokens_out_internal'] = np.where((transfers['internal_external']=='aave_internal') & (transfers['transfer_type'] == 'OUT'), transfers['amount_transferred'], float(0))
                       

            # aggregate transfers to collector
            transfers = transfers[['market','collector','block_day','tokens_in_external','tokens_in_internal','tokens_out_external','tokens_out_internal']]
            transfers = transfers.groupby(['market','collector','block_day']).sum().reset_index() 

            # join transfers to main table
            return_val = return_val.merge(transfers, how='left')

        else:
            return_val['tokens_in_external'] = float(0)
            return_val['tokens_in_internal'] = float(0)
            return_val['tokens_out_external'] = float(0)
            return_val['tokens_out_internal'] = float(0)


        if not v3_minted_to_treasury_by_day.empty:
            v3_minted_to_treasury_by_day = v3_minted_to_treasury_by_day[['market','atoken','atoken_symbol','block_height','block_day','minted_to_treasury_amount','minted_amount']].copy()
            v3_minted_to_treasury_by_day.rename(columns={'atoken':'token','atoken_symbol':'symbol'}, inplace=True)
            return_val = return_val.merge(v3_minted_to_treasury_by_day, how='left')
        else:
            return_val['minted_to_treasury_amount'] = float(0)
            return_val['minted_amount'] = float(0)
        
        return_val = return_val.fillna(float(0))
        return_val['chain'] = chain
        return_val = standardise_types(return_val)
        
        
    else:
        return_val = pd.DataFrame()
    
    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )
    return return_val

@asset(
    compute_kind='python',
    partitions_def=market_day_multipartition,
    group_name='data_warehouse',
    code_version="1",
    io_manager_key = 'data_warehouse_io_manager',
    ins={
        "non_atoken_balances_by_day": AssetIn(key_prefix="financials_data_lake"),
        "non_atoken_transfers_by_day": AssetIn(key_prefix="financials_data_lake"),
        "internal_external_addresses": AssetIn(key_prefix="financials_data_lake"),
    }
)
def non_atoken_measures_by_day(
            context,
            non_atoken_balances_by_day,
            non_atoken_transfers_by_day,
            internal_external_addresses
            ) -> pd.DataFrame:
    """
    Joins all measures relevant to the non-atokens into one table

    Args:
        context: dagster context object
        non_atoken_balances_by_day: the output of non_atoken_balances_by_day
        non_atoken_transfers_by_day: the output of non_atoken_transfers_by_day
        internal_external_addresses: the output of internal_external_addresses

    Returns:
        A dataframe with a row for each non-atoken on the day, and the output of all the measures
        joined to the token metadata.  Non existing measures are set to 0

    """
    date, market = context.partition_key.split("|")
    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")
    chain = CONFIG_MARKETS[market]['chain']

    return_val = non_atoken_balances_by_day
    if not return_val.empty:
        
        if not non_atoken_transfers_by_day.empty:
            # transfers need to be classified as internal/external and aggregated to day level
            transfers = non_atoken_transfers_by_day.copy()
            transfers.columns = transfers.columns.str.replace('transfers_','')

            transfers = transfers[[
                    'market',
                    'collector',
                    'transfer_type',
                    'from_address',
                    'to_address',
                    'contract_address',
                    'contract_symbol',
                    'block_day',
                    'amount_transferred'
            ]]
            transfers.rename(columns={'contract_address':'token','contract_symbol':'symbol', "collector":"contract_address"}, inplace=True)

            # classify transfers as internal or external
            transfers_in = transfers.loc[transfers['transfer_type']=='IN']
            transfers_out = transfers.loc[transfers['transfer_type']=='OUT']
            transfer_class = internal_external_addresses.loc[internal_external_addresses['chain']==chain]
            transfer_class = transfer_class[['contract_address','internal_external']].copy()

            if not transfers_in.empty:
                    transfer_class_in = transfer_class.rename(columns={'contract_address':'from_address'})
                    transfers_in = transfers_in.merge(transfer_class_in, how='left')
            
            if not transfers_out.empty:
                    transfer_class_out = transfer_class.rename(columns={'contract_address':'to_address'})
                    transfers_out = transfers_out.merge(transfer_class_out, how='left')

            transfers = pd.concat([transfers_in, transfers_out])
            transfers.internal_external.fillna('aave_external', inplace=True)
            
            # create columns for internal and external transfers
            transfers['tokens_in_external'] = np.where((transfers['internal_external']=='aave_external') & (transfers['transfer_type'] == 'IN'), transfers['amount_transferred'], float(0))
            transfers['tokens_in_internal'] = np.where((transfers['internal_external']=='aave_internal') & (transfers['transfer_type'] == 'IN'), transfers['amount_transferred'], float(0))
            transfers['tokens_out_external'] = np.where((transfers['internal_external']=='aave_external') & (transfers['transfer_type'] == 'OUT'), transfers['amount_transferred'], float(0))
            transfers['tokens_out_internal'] = np.where((transfers['internal_external']=='aave_internal') & (transfers['transfer_type'] == 'OUT'), transfers['amount_transferred'], float(0))
                       

            # aggregate transfers to collector
            transfers = transfers[['market','contract_address','block_day','tokens_in_external','tokens_in_internal','tokens_out_external','tokens_out_internal']]
            transfers = transfers.groupby(['market','contract_address','block_day']).sum().reset_index() 

            # join transfers to main table
            return_val = return_val.merge(transfers, how='left')

        else:
            return_val['tokens_in_external'] = float(0)
            return_val['tokens_in_internal'] = float(0)
            return_val['tokens_out_external'] = float(0)
            return_val['tokens_out_internal'] = float(0)
        # ic(return_val)
        return_val = return_val.fillna(float(0))
        return_val = standardise_types(return_val)
        
        
    else:
        return_val = pd.DataFrame()
    
    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )
    return return_val


@asset(
    compute_kind='python',
    partitions_def=market_day_multipartition,
    group_name='data_warehouse',
    code_version="1",
    io_manager_key = 'data_warehouse_io_manager',
    ins={
        "user_lm_rewards_claimed": AssetIn(key_prefix="financials_data_lake"),
    }
)
def user_rewards_by_day(
            context,
            user_lm_rewards_claimed
            ) -> pd.DataFrame:
    """
    Joins all measures relevant to the user LM and SM rewards into one table
    Covers Aave liquidity mining (LM) rewards and Safety Module staking rewards (SM)

    Args:
        context: dagster context object
        user_lm_rewards_claimed: the output of user_lm_rewards_claimed

    Returns:
        A dataframe with a row for each non-atoken on the day, and the output of all the measures
        joined to the token metadata.  Non existing measures are set to 0

    """
    date, market = context.partition_key.split("|")
    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")
    chain = CONFIG_MARKETS[market]['chain']

    if not user_lm_rewards_claimed.empty:
        return_val = user_lm_rewards_claimed
        # todo: join unclaimed rewards table when implemented
        return_val['sm_stkAAVE_owed'] = float(0)
        return_val['sm_stkABPT_owed'] = float(0)
        return_val['lm_aave_v2_owed'] = float(0)
    else:
        return_val = pd.DataFrame()

    return_val = standardise_types(return_val)

    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )
    return return_val

@asset(
    compute_kind='python',
    partitions_def=market_day_multipartition,
    group_name='data_warehouse',
    code_version="1",
    io_manager_key = 'data_warehouse_io_manager',
    ins={
        "treasury_accrued_incentives_by_day": AssetIn(key_prefix="financials_data_lake"),
    }
)
def treasury_incentives_by_day(
            context,
            treasury_accrued_incentives_by_day
            ) -> pd.DataFrame:
    """
    Joins all measures relevant to the LM rewards owed and owned by treasury contracts


    Args:
        context: dagster context object
        treasury_accrued_incentives_by_day: the output of treasury_accrued_incentives_by_day

    Returns:
        A dataframe with a row for each non-atoken on the day, and the output of all the measures
        joined to the token metadata.  Non existing measures are set to 0

    """
    date, market = context.partition_key.split("|")
    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")
    chain = CONFIG_MARKETS[market]['chain']

    if not treasury_accrued_incentives_by_day.empty:
        return_val = treasury_accrued_incentives_by_day[[
            'chain',
            'market',
            'collector_contract',
            'block_day',
            'rewards_token_address',
            'rewards_token_symbol',
            'accrued_rewards',
        ]]

        # todo: join held rewards table when implemented
        return_val['held_rewards'] = float(0)
    else:
        return_val = pd.DataFrame()

    return_val = standardise_types(return_val)

    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )

    return return_val

if __name__ == "__main__":
    # test_blocks_by_day()
    # test_atoken_measures_by_day()
    import pandas as pd
    poly = pd.read_pickle('/workspaces/aave-etl/storage/collector_atoken_transfers_by_day/2023-01-19|polygon_v3')
    wbtc = poly.loc[poly.transfers_contract_symbol == 'aPolWBTC']
    ic(wbtc)
    print(wbtc.to_dict())




