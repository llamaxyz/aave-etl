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
                    AllPartitionMapping,
                    IdentityPartitionMapping,
                    AssetIn
                     )
from icecream import ic

from aave_data.resources.financials_config import * #pylint: disable=wildcard-import, unused-wildcard-import

from aave_data.resources.helpers import (
    standardise_types
)
from aave_data.assets.financials.data_lake import (
    market_day_multipartition,
)

if not sys.warnoptions:
    import warnings
    warnings.filterwarnings("ignore", category=ExperimentalWarning)




@asset(
    compute_kind='python',
    #group_name='data_warehouse',
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

    return_val = block_numbers_by_day[['block_day','block_time','block_height','end_block','chain']].drop_duplicates()

    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )
    return return_val


@asset(
    compute_kind='python',
    #group_name='data_warehouse',
    code_version="1",
    io_manager_key = 'data_warehouse_io_manager',
    ins={
        "collector_atoken_balances_by_day": AssetIn(key_prefix="financials_data_lake"),
        "collector_atoken_transfers_by_day": AssetIn(key_prefix="financials_data_lake"),
        "v3_accrued_fees_by_day": AssetIn(key_prefix="financials_data_lake"),
        "v3_minted_to_treasury_by_day": AssetIn(key_prefix="financials_data_lake"),
        "aave_internal_addresses": AssetIn(key_prefix="warehouse"),
    }
)
def atoken_measures_by_day(
            context,
            collector_atoken_balances_by_day,
            collector_atoken_transfers_by_day,
            v3_accrued_fees_by_day,
            v3_minted_to_treasury_by_day,
            aave_internal_addresses
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

    mc = []
    for market in CONFIG_MARKETS.keys():
        mc.append([market, CONFIG_MARKETS[market]['chain']])
    mc = pd.DataFrame(mc, columns=['market','chain'])


    return_val = collector_atoken_balances_by_day
    

    if not return_val.empty:
        return_val = return_val.merge(mc, how='left')

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
            transfers = transfers.merge(mc, how='left')

            transfers = transfers[[
                    'market',
                    'chain',
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
            # transfer_class = internal_external_addresses.loc[internal_external_addresses['chain']==chain]
            transfer_class = aave_internal_addresses[['chain','contract_address','internal_external']].copy()

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
            transfers = transfers[['market','chain','collector','block_day','token','symbol','tokens_in_external','tokens_in_internal','tokens_out_external','tokens_out_internal']]
            transfers = transfers.groupby(['market','chain','collector','token','symbol','block_day']).sum().reset_index() 

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

        # fix up eth_v1 ETH token symbol - should be WETH
        return_val['symbol'] = np.where((return_val['market']=='ethereum_v1') & (return_val['symbol'] == 'ETH'), 'WETH', return_val['symbol'])
        
        return_val = return_val.fillna(float(0))
        # return_val['chain'] = chain
        # return_val = standardise_types(return_val)
        
        
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
    #group_name='data_warehouse',
    code_version="1",
    io_manager_key = 'data_warehouse_io_manager',
    ins={
        "non_atoken_balances_by_day": AssetIn(key_prefix="financials_data_lake"),
        "non_atoken_transfers_by_day": AssetIn(key_prefix="financials_data_lake"),
        "aave_internal_addresses": AssetIn(key_prefix="warehouse"),
        "paraswap_fees": AssetIn(key_prefix="warehouse"),
    }
)
def non_atoken_measures_by_day(
            context,
            non_atoken_balances_by_day,
            non_atoken_transfers_by_day,
            aave_internal_addresses,
            paraswap_fees
            ) -> pd.DataFrame:
    """
    Joins all measures relevant to the non-atokens into one table

    Args:
        context: dagster context object
        non_atoken_balances_by_day: the output of non_atoken_balances_by_day
        non_atoken_transfers_by_day: the output of non_atoken_transfers_by_day
        internal_external_addresses: the output of internal_external_addresses
        paraswap_fees: the output of paraswap_fees

    Returns:
        A dataframe with a row for each non-atoken on the day, and the output of all the measures
        joined to the token metadata.  Non existing measures are set to 0

    """
    mc = []
    for market in CONFIG_MARKETS.keys():
        mc.append([market, CONFIG_MARKETS[market]['chain']])
    mc = pd.DataFrame(mc, columns=['market','chain'])

    return_val = non_atoken_balances_by_day
    return_val.drop(columns=['block_height', 'decimals'], inplace=True)
    if not return_val.empty:
        return_val = return_val.merge(mc, how='left')
        if not non_atoken_transfers_by_day.empty:
            # transfers need to be classified as internal/external and aggregated to day level
            transfers = non_atoken_transfers_by_day.copy()
            transfers.columns = transfers.columns.str.replace('transfers_','')
            transfers = transfers.merge(mc, how='left')

            transfers = transfers[[
                    'market',
                    'chain',
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
            # transfer_class = internal_external_addresses.loc[internal_external_addresses['chain']==chain]
            transfer_class = aave_internal_addresses[['chain','contract_address','internal_external']].copy()

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
            transfers = transfers[['market','chain','contract_address','token','block_day','tokens_in_external','tokens_in_internal','tokens_out_external','tokens_out_internal']]
            transfers = transfers.groupby(['market','chain','contract_address','token','block_day']).sum().reset_index() 

            # join transfers to main table
            return_val = return_val.merge(transfers, how='left')

            # add paraswap fees by concat and sum
            paraswap_fees.rename(
                columns={
                    'reserve':'token',
                    'paraswap_fee_claimer':'contract_address',
                    'claimable':'paraswap_fees_claimable'
                }, 
                inplace=True
            )

            return_val = pd.concat([return_val, paraswap_fees])
            return_val = return_val.fillna(float(0))
            return_val = return_val.groupby(['block_day','chain','market','contract_address','token','symbol',]).sum().reset_index()

        else:
            return_val['tokens_in_external'] = float(0)
            return_val['tokens_in_internal'] = float(0)
            return_val['tokens_out_external'] = float(0)
            return_val['tokens_out_internal'] = float(0)
            return_val['paraswap_fees_claimable'] = float(0)
        # ic(return_val)
        return_val = return_val.fillna(float(0))
        
        
        
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
    #group_name='data_warehouse',
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
    #group_name='data_warehouse',
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


@asset(
    compute_kind='python',
    #group_name='data_warehouse',
    code_version="1",
    io_manager_key = 'data_warehouse_io_manager',
    ins={
        "aave_oracle_prices_by_day": AssetIn(
            key_prefix="financials_data_lake",
            # partition_mapping=AllPartitionMapping()
            ),
    }
)
def token_prices_by_day(
            context,
            aave_oracle_prices_by_day
            ) -> pd.DataFrame:
    """
    Returns the prices of all tokens on the day, collated from the Aave oracle
      and other sources

    Price rank field from config is used to pick one price when multiple are available on 
    the same chain.  For example, USDC is in v1, v2, v3, rwa, arc on ethereum.
    Similar situation for WAVAX on avax v2 and v3.  Aave oracles don't always
    return the same prices because some are in wei and some in usd.  Wei prices
    are converted using eth/usd from mainnet.


    Args:
        context: dagster context object
        aave_oracle_prices_by_day: the output of aave_oracle_prices_by_day

    Returns:
        A dataframe with a row for each reserve token on the day with usd pricing

    """
    return_val = aave_oracle_prices_by_day
    return_val['pricing_source'] = 'aave_oracle'

    market_chain = []
    for market in CONFIG_MARKETS.keys():
        chain = CONFIG_MARKETS[market]['chain']
        price_rank = CONFIG_MARKETS[market]['price_rank']
        market_chain.append((market, chain, price_rank))
    
    market_chain = pd.DataFrame(market_chain, columns=['market', 'chain', 'price_rank'])
    return_val = return_val.merge(market_chain, on='market', how='left')

    # group by chain, reserve, symbol, block_day and calc the min price rank
    min_ranks = return_val.groupby(['chain', 'reserve', 'symbol', 'block_day']).agg(
        min_rank = ('price_rank', 'min')
    ).reset_index()

    # join the min ranks to the return val
    return_val = return_val.merge(min_ranks, on=['chain', 'reserve', 'symbol', 'block_day'], how='left')

    # filter to only the min rank
    return_val = return_val.loc[return_val.price_rank == return_val.min_rank]

    return_val = return_val[['block_day', 'chain', 'reserve', 'symbol', 'usd_price', 'pricing_source']].copy()
    return_val = return_val.drop_duplicates().reset_index(drop=True)

    return_val = standardise_types(return_val)

    # ic(return_val.shape)
    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )

    return return_val


@asset(
    compute_kind='python',
    #group_name='data_warehouse',
    code_version="1",
    io_manager_key = 'data_warehouse_io_manager',
    ins={
        "market_tokens_by_day": AssetIn(key_prefix="financials_data_lake"),
        "internal_external_addresses": AssetIn(key_prefix="financials_data_lake"),
    }
)
def aave_internal_addresses(
            context,
            market_tokens_by_day,
            internal_external_addresses
            ) -> pd.DataFrame:
    """
    Takes the ouput of internal_external_addresses (from manual CSV upload)
     and adds all atoken addresses to it
    This is used to flag transactions as internal to Aave when there
     are wrap/unrwap transactions with atoken contracts


    Args:
        context: dagster context object
        market_tokens_by_day: the output of market_tokens_by_day
        internal_external_addresses: the output of internal_external_addresses

    Returns:
        A dataframe of manual addresses from the CSV and all atokens

    """
    mc = []
    for market in CONFIG_MARKETS.keys():
        mc.append([market, CONFIG_MARKETS[market]['chain']])
    mc = pd.DataFrame(mc, columns=['market','chain'])

    atokens = market_tokens_by_day.merge(mc, on='market', how='left')
    atokens = atokens[['chain','atoken_symbol','atoken']].copy()
    atokens.drop_duplicates(inplace=True)
    atokens.rename(columns={'atoken_symbol':'label', 'atoken':'contract_address'}, inplace=True)

    atokens['internal_external'] = 'aave_internal'

    return_val = pd.concat([internal_external_addresses, atokens], ignore_index=True)

    return_val = standardise_types(return_val)

    # ic(return_val.shape)
    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )

    return return_val

@asset(
    compute_kind='python',
    code_version="1",
    io_manager_key = 'data_warehouse_io_manager',
    ins={
        "market_tokens_by_day": AssetIn(key_prefix="financials_data_lake"),
        "balance_group_lists": AssetIn(key_prefix="financials_data_lake"),
        "eth_balances_by_day": AssetIn(key_prefix="financials_data_lake"),
    }
)
def balance_group_lookup(
            context,
            market_tokens_by_day,
            balance_group_lists,
            eth_balances_by_day
            ) -> pd.DataFrame:
    """
    Converts the balance_group_lists into a lookup table
    
    Args:
        context: dagster context object
        market_tokens_by_day: the output of market_tokens_by_day
        internal_external_addresses: the output of internal_external_addresses

    Returns:
        A dataframe of manual addresses from the CSV and all atokens

    """
    mc = []
    for market in CONFIG_MARKETS.keys():
        mc.append([market, CONFIG_MARKETS[market]['chain']])
    mc = pd.DataFrame(mc, columns=['market','chain'])

    # build the tokens table
    return_val = market_tokens_by_day[['market','atoken','atoken_symbol','reserve','symbol',]].copy().drop_duplicates()
    # add the V1 tokens as native reserves
    v1_tokens = return_val.loc[return_val.market == 'ethereum_v1'].copy()
    v1_tokens.atoken = v1_tokens.reserve
    return_val = pd.concat([return_val, v1_tokens])
    # add the non-atokens
    non_atokens = pd.DataFrame()
    for market in CONFIG_TOKENS.keys():
        for contract in CONFIG_TOKENS[market].keys():
            for token in CONFIG_TOKENS[market][contract]['tokens'].keys():
                token_row = pd.DataFrame(
                    [
                        {
                            'market': market,
                            'atoken': CONFIG_TOKENS[market][contract]['tokens'][token]['address'],
                            'atoken_symbol': token,
                            'reserve': CONFIG_TOKENS[market][contract]['tokens'][token]['address'],
                            'symbol': token,
                        }
                    ]
                )
                non_atokens = pd.concat([non_atokens, token_row])
    non_atokens = non_atokens.drop_duplicates()
    return_val = pd.concat([return_val, non_atokens])
    # add the gas tokens
    gas_tokens = eth_balances_by_day[['market','wrapped_gas_token','gas_token']].copy().drop_duplicates()
    gas_tokens.rename(columns={'wrapped_gas_token':'atoken', 'gas_token':'atoken_symbol'}, inplace=True)
    gas_tokens['reserve'] = gas_tokens['atoken']
    gas_tokens['symbol'] = gas_tokens['atoken_symbol']
    return_val = pd.concat([return_val, gas_tokens])
    # merge the chain names
    return_val = return_val.merge(mc, on='market', how='left')
    
    # get the list of balance groups and assign them
    return_val['balance_group'] = pd.NA
    for col in balance_group_lists.columns:
        token_list = balance_group_lists[col].dropna().tolist()
        return_val['balance_group'] = np.where(return_val['atoken_symbol'].isin(token_list), col, return_val['balance_group'])
    
    return_val['balance_group'] = np.where(return_val['balance_group'].isna(), 'Other Token', return_val['balance_group'])
    return_val['stable_class'] = np.where(return_val['balance_group'].isin(['DAI','USDC','USDT','other_stables']), 'stablecoin', 'unstablecoin')
    return_val['balance_group'] = np.where(return_val['balance_group'] == 'other_stables', 'Other Stables', return_val['balance_group'])


    return_val = standardise_types(return_val)

    # ic(return_val.shape)
    context.add_output_metadata(
        {
            "num_records": len(return_val),
            "preview": MetadataValue.md(return_val.head().to_markdown()),
        }
    )

    return return_val

@asset(
    compute_kind='python',
    code_version="1",
    io_manager_key = 'data_warehouse_io_manager',
    ins={
        "paraswap_claimable_fees": AssetIn(key_prefix="financials_data_lake"),
    }
)
def paraswap_fees(
            context,
            paraswap_claimable_fees,
            ) -> pd.DataFrame:
    """
    Deduplicates the paraswap fees table
    
    Args:
        context: dagster context object
        paraswap_claimable_fees: the output of paraswap_claimable_fees

    Returns:
        A dataframe of outstanding paraswap fees to be claimed

    """

    # get the latest day from the table
    latest_day = paraswap_claimable_fees.block_day.max()
    context.log.info(f'block day {latest_day}')

    fees = paraswap_claimable_fees[['block_day','chain','market','paraswap_fee_claimer','reserve','symbol','claimable']]

    # deduplicate the fees
    fees = fees.sort_values(['block_day','chain','reserve','market','paraswap_fee_claimer'], ascending=[True,True,True,True,True]).drop_duplicates(['block_day','chain','paraswap_fee_claimer','reserve',], keep='last')
    
    # drop the zeroes
    fees = fees.loc[fees.claimable > 0]

    context.add_output_metadata(
        {
            "num_records": len(fees),
            "preview": MetadataValue.md(fees.head().to_markdown()),
        }
    )

    return fees

# if __name__ == "__main__":
    # test_blocks_by_day()
    # test_atoken_measures_by_day()
    # import pandas as pd
    # poly = pd.read_pickle('/workspaces/aave-etl/storage/collector_atoken_transfers_by_day/2023-01-19|polygon_v3')
    # wbtc = poly.loc[poly.transfers_contract_symbol == 'aPolWBTC']
    # ic(wbtc)
    # print(wbtc.to_dict())





