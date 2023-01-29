import sys
import pandas as pd
from dagster import (#AssetIn,  # SourceAsset,; Output,
                    #  DailyPartitionsDefinition, 
                     ExperimentalWarning,
                     MetadataValue, #MultiPartitionKey,
                    #  MultiPartitionsDefinition, Optional, PartitionKeyRange,
                    #  PartitionMapping, PartitionsDefinition,
                    #  StaticPartitionsDefinition, 
                    asset,# op,
                    #  LastPartitionMapping
                     )
from icecream import ic

from financials.financials_config import * #pylint: disable=wildcard-import, unused-wildcard-import

# from aave.resources.partition_maps import (ChainToMarketPartitionMap,
#                                            SingleChainPartitionMap,
#                                            MarketToDatePartitionMap)


from financials.assets.data_lake import (
    market_day_multipartition,
    # v3_market_day_multipartition
)


if not sys.warnoptions:
    import warnings
    warnings.filterwarnings("ignore", category=ExperimentalWarning)


# @asset(
#     compute_kind='python',
#     group_name='data_warehouse'
# )
# def market_tokens_table(context, market_tokens_by_day) -> pd.DataFrame: #pylint: disable=W0621
#     """Table of the tokens and metadata in a market for all block heights

#     Aggregates all the market_tokens_by_day tables into a single table
#     and adds the first seen block height for each token

#     Args:
#         context: dagster context object
#         market_tokens_by_day: the output of market_tokens_by_day for all markets and block heights

#     Returns:
#         A dataframe with market reserve & atoken details plus first seen block height

#     """
#     # ic(market_tokens_by_day)
#     combined = pd.DataFrame()
#     for value in market_tokens_by_day.values():
#         combined = pd.concat([combined, value])

#     minblock = combined.groupby([col for col in combined.columns if col not in ['block_height','block_day']]).min().reset_index()
#     minblock.rename(columns={"block_height": "first_seen_block"}, inplace=True)
#     minblock.rename(columns={"block_day": "first_seen_day"}, inplace=True)

#     # force checksummed addresses to lower case
#     minblock.atoken = minblock.atoken.str.lower()
#     minblock.pool = minblock.pool.str.lower()
#     minblock.reserve = minblock.reserve.str.lower()

#     # explicitly set types
#     minblock.atoken = minblock.atoken.astype(pd.StringDtype()) # type: ignore
#     minblock.atoken_decimals = minblock.atoken_decimals.astype('int64')
#     minblock.decimals = minblock.decimals.astype('int64')
#     minblock.first_seen_block = minblock.first_seen_block.astype('int64')
#     minblock.first_seen_day = minblock.first_seen_day.dt.tz_localize('UTC')
#     minblock.market = minblock.market.astype(pd.StringDtype()) # type: ignore
#     minblock.name = minblock.name.astype(pd.StringDtype()) # type: ignore
#     minblock.pool = minblock.pool.astype(pd.StringDtype()) # type: ignore
#     minblock.reserve = minblock.reserve.astype(pd.StringDtype()) # type: ignore
#     minblock.symbol = minblock.symbol.astype(pd.StringDtype()) # type: ignore
#     minblock.atoken_symbol = minblock.atoken_symbol.astype(pd.StringDtype()) # type: ignore

#     context.add_output_metadata(
#         {
#             "num_records": len(minblock),
#             "preview": MetadataValue.md(minblock.head().to_markdown()),
#         }
#     )
#     return minblock

@asset(
    partitions_def=market_day_multipartition,
    compute_kind="python",
    group_name='data_warehouse'
    # io_manager_def=
)
def aave_oracle_prices_table(context, aave_oracle_prices_by_day) -> pd.DataFrame:  # type: ignore pylint: disable=W0621
    """
    Table of the token and aave oracle price for each market at each block height
    This asset will be materialised to the database

    Args:
        aave_oracle_prices_by_day: the output of aave_oracle_prices_by_day for a given market
    
    Returns:
        A dataframe market, token and the underlying reserve oracle price at the block height, 
            with types explicitly set and addresses set to lowercase

    """
    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")
    
    prices = aave_oracle_prices_by_day.copy()

    if not prices.empty:
        # set the types explicitly
        prices.reserve = prices.reserve.astype(pd.StringDtype()) # type: ignore
        prices.symbol = prices.symbol.astype(pd.StringDtype()) # type: ignore
        prices.market = prices.market.astype(pd.StringDtype()) # type: ignore
        prices.block_height = prices.block_height.astype('int64')
        prices.block_day = prices.block_day.dt.tz_localize('UTC')
        prices.usd_price = prices.usd_price.astype('float64')

        # force checksum addresses to lowercase
        prices.reserve = prices.reserve.str.lower()

    context.add_output_metadata(
        {
            "num_records": len(prices),
            "preview": MetadataValue.md(prices.head().to_markdown()),
        }
    )

    return prices

@asset(
    partitions_def=market_day_multipartition,
    compute_kind="python",
    group_name='data_warehouse'
)
def collector_atoken_transfers_table(context, collector_atoken_transfers_by_day) -> pd.DataFrame:  # type: ignore pylint: disable=W0621
    """
    Table of the aave market token transfers in & out of the collector contracts for each market
    This table will be materialised to database
    This table has types set explicitly and addresses set to lowercase to ensure DB compatibility
    
    Args:
        context: dagster context object
        collector_atoken_transfers_by_day: the output of collector_atoken_transfers_by_day for a given market
    Returns:
        A dataframe with the token transfers in and out of the collector contracts for each market
    """

    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")

    transfers = collector_atoken_transfers_by_day

    if not transfers.empty:
        # set the types explicitly
        transfers.transfers_transfer_type = transfers.transfers_transfer_type.astype(pd.StringDtype()) # type: ignore
        transfers.transfers_from_address = transfers.transfers_from_address.astype(pd.StringDtype()) # type: ignore
        transfers.transfers_to_address = transfers.transfers_to_address.astype(pd.StringDtype()) # type: ignore
        transfers.transfers_contract_address = transfers.transfers_contract_address.astype(pd.StringDtype()) # type: ignore
        transfers.transfers_contract_name = transfers.transfers_contract_name.astype(pd.StringDtype()) # type: ignore
        transfers.transfers_contract_decimals = transfers.transfers_contract_decimals.astype('int64')
        transfers.transfers_contract_symbol = transfers.transfers_contract_symbol.astype(pd.StringDtype()) # type: ignore
        transfers.block_day = pd.to_datetime(transfers.block_day, utc=True)
        transfers.amount_transferred = transfers.amount_transferred.astype('float64')
        transfers.start_block = transfers.start_block.astype('int64')
        transfers.end_block = transfers.end_block.astype('int64')

        # force checksum addresses to lowercase
        transfers.transfers_from_address = transfers.transfers_from_address.str.lower()
        transfers.transfers_to_address = transfers.transfers_to_address.str.lower()
        transfers.transfers_contract_address = transfers.transfers_contract_address.str.lower()

    context.add_output_metadata(
        {
            "num_records": len(transfers),
            "preview": MetadataValue.md(transfers.head().to_markdown()),
        }
    )

    return transfers

@asset(
    partitions_def=market_day_multipartition,
    compute_kind="python",
    group_name='data_warehouse'
)
def non_atoken_transfers_table(context, non_atoken_transfers_by_day) -> pd.DataFrame:  # type: ignore pylint: disable=W0621
    """
    Table of the non atoken transfers in & out of the collector contracts for each market
    This table will be materialised to database
    This table has types set explicitly and addresses set to lowercase to ensure DB compatibility
    
    Args:
        context: dagster context object
        non_atoken_transfers_by_day: the output of non_atoken_transfers_by_day for a given market
    Returns:
        A dataframe with the token transfers in and out of the collector contracts for each market
    """

    
    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")

    transfers = non_atoken_transfers_by_day

    if not transfers.empty:
        # set the types explicitly
        transfers.transfers_transfer_type = transfers.transfers_transfer_type.astype(pd.StringDtype()) # type: ignore
        transfers.transfers_from_address = transfers.transfers_from_address.astype(pd.StringDtype()) # type: ignore
        transfers.transfers_to_address = transfers.transfers_to_address.astype(pd.StringDtype()) # type: ignore
        transfers.transfers_contract_address = transfers.transfers_contract_address.astype(pd.StringDtype()) # type: ignore
        transfers.transfers_contract_name = transfers.transfers_contract_name.astype(pd.StringDtype()) # type: ignore
        transfers.transfers_contract_decimals = transfers.transfers_contract_decimals.astype('int64')
        transfers.transfers_contract_symbol = transfers.transfers_contract_symbol.astype(pd.StringDtype()) # type: ignore
        transfers.block_day = pd.to_datetime(transfers.block_day, utc=True)
        transfers.amount_transferred = transfers.amount_transferred.astype('float64')
        transfers.start_block = transfers.start_block.astype('int64')
        transfers.end_block = transfers.end_block.astype('int64')
        transfers.wallet = transfers.wallet.astype(pd.StringDtype()) # type: ignore

        # force checksum addresses to lowercase
        transfers.transfers_from_address = transfers.transfers_from_address.str.lower()
        transfers.transfers_to_address = transfers.transfers_to_address.str.lower()
        transfers.transfers_contract_address = transfers.transfers_contract_address.str.lower()
        transfers.wallet = transfers.wallet.str.lower()

    context.add_output_metadata(
        {
            "num_records": len(transfers),
            "preview": MetadataValue.md(transfers.head().to_markdown()),
        }
    )

    return transfers

@asset(
    partitions_def=market_day_multipartition,
    compute_kind="python",
    group_name='data_warehouse'
)
def collector_atoken_balances_table(context, collector_atoken_balances_by_day) -> pd.DataFrame:  # type: ignore pylint: disable=W0621
    """
    Table of the collector contract balances at each day in each market
    This table will be materialised to database
    This table has types set explicitly and addresses set to lowercase to ensure DB compatibility
    
    Args:
        context: dagster context object
        collector_atoken_balances_by_day: the output of collector_atoken_balances_by_day for a given market
    Returns:
        A dataframe with the token transfers in and out of the collector contracts for each market

    """
    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")

    bals = collector_atoken_balances_by_day

    if not bals.empty:
        # set the types explicitly
        bals.collector = bals.collector.astype(pd.StringDtype()) # type: ignore
        bals.market = bals.market.astype(pd.StringDtype()) # type: ignore
        bals.token = bals.token.astype(pd.StringDtype()) # type: ignore
        bals.symbol = bals.symbol.astype(pd.StringDtype()) # type: ignore
        bals.block_height = bals.block_height.astype('int64')
        bals.block_day = pd.to_datetime(bals.block_day, utc=True)
        bals.balance = bals.balance.astype('float64')

        # force checksum addresses to lowercase
        bals.collector = bals.collector.str.lower()
        bals.token = bals.token.str.lower()

    context.add_output_metadata(
        {
            "num_records": len(bals),
            "preview": MetadataValue.md(bals.head().to_markdown()),
        }
    )

    return bals

@asset(
    partitions_def=market_day_multipartition,
    compute_kind="python",
    group_name='data_warehouse'
)
def non_atoken_balances_table(context, non_atoken_balances_by_day) -> pd.DataFrame:  # type: ignore pylint: disable=W0621
    """
    Table of the non-atoken balances at each day in each chain
    This table will be materialised to database
    This table has types set explicitly and addresses set to lowercase to ensure DB compatibility
    
    Args:
        context: dagster context object
        non_atoken_balances_by_day: the output of non_atoken_balances_by_day for a given chain
    Returns:
        A dataframe with the token transfers in and out of the collector contracts for each chain
    """
    # market = context.partition_key.keys_by_dimension['market']
    # # chain = CONFIG_MARKETS[market]['chain']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")

    bals = non_atoken_balances_by_day

    if not bals.empty:
        # set the types explicitly
        bals.wallet = bals.wallet.astype(pd.StringDtype()) # type: ignore
        bals.chain = bals.chain.astype(pd.StringDtype()) # type: ignore
        bals.market = bals.market.astype(pd.StringDtype()) # type: ignore
        bals.token = bals.token.astype(pd.StringDtype()) # type: ignore
        bals.decimals = bals.decimals.astype('int64')
        bals.symbol = bals.symbol.astype(pd.StringDtype()) # type: ignore
        bals.block_height = bals.block_height.astype('int64')
        bals.block_day = pd.to_datetime(bals.block_day, utc=True)
        bals.balance = bals.balance.astype('float64')

        # force checksum addresses to lowercase
        bals.wallet = bals.wallet.str.lower()
        bals.token = bals.token.str.lower()

    context.add_output_metadata(
        {
            "num_records": len(bals),
            "preview": MetadataValue.md(bals.head().to_markdown()),
        }
    )

    return bals


@asset(
    # partitions_def=v3_market_day_multipartition,
    partitions_def=market_day_multipartition,
    compute_kind="python",
    group_name='data_warehouse'
)
def v3_accrued_fees_table(context, v3_accrued_fees_by_day) -> pd.DataFrame:  # type: ignore pylint: disable=W0621
    """
    Table of the accrued fees for aave v3
    This table will be materialised to database
    This table has types set explicitly and addresses set to lowercase to ensure DB compatibility
    
    Args:
        context: dagster context object
        v3_accrued_fees_by_day: the output of v3_accrued_fees_by_day
    Returns:
        A dataframe with the accrued v3 fees for each market and reserve token
    """
    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")

    fees = v3_accrued_fees_by_day

    if not fees.empty:
        # set the types explicitly
        fees.market = fees.market.astype(pd.StringDtype()) # type: ignore
        fees.reserve = fees.reserve.astype(pd.StringDtype()) # type: ignore
        fees.symbol = fees.symbol.astype(pd.StringDtype()) # type: ignore
        fees.atoken = fees.atoken.astype(pd.StringDtype()) # type: ignore
        fees.atoken_symbol = fees.atoken_symbol.astype(pd.StringDtype()) # type: ignore
        fees.block_height = fees.block_height.astype('int')
        fees.block_day = pd.to_datetime(fees.block_day, utc=True)
        fees.accrued_fees = fees.accrued_fees.astype('float')

        # force checksum addresses to lowercase
        fees.reserve = fees.reserve.str.lower()
        fees.atoken = fees.atoken.str.lower()

    context.add_output_metadata(
        {
            "num_records": len(fees),
            "preview": MetadataValue.md(fees.head().to_markdown()),
        }
    )

    return fees

@asset(
    # partitions_def=v3_market_day_multipartition,
    partitions_def=market_day_multipartition,
    compute_kind="python",
    group_name='data_warehouse'
)
def v3_minted_to_treasury_table(context, v3_minted_to_treasury_by_day) -> pd.DataFrame:
    """
    Table of the minted_to_treasury event data for aave v3
    This table will be materialised to database
    This table has types set explicitly and addresses set to lowercase to ensure DB compatibility
    
    Args:
        context: dagster context object
        v3_minted_to_treasury_by_day: the output of v3_minted_to_treasury_by_day
    Returns:
        A dataframe with the minted_to_treasury event data for each market and reserve token
    """
    # market = context.partition_key.keys_by_dimension['market']
    # date = context.partition_key.keys_by_dimension['date']
    date, market = context.partition_key.split("|")
    context.log.info(f"market: {market}")
    context.log.info(f"date: {date}")

    minted = v3_minted_to_treasury_by_day

    if not minted.empty:
        # set the types explicitly
        minted.market = minted.market.astype(pd.StringDtype()) # type: ignore
        minted.reserve = minted.reserve.astype(pd.StringDtype()) # type: ignore
        minted.symbol = minted.symbol.astype(pd.StringDtype()) # type: ignore
        minted.atoken = minted.atoken.astype(pd.StringDtype()) # type: ignore
        minted.atoken_symbol = minted.atoken_symbol.astype(pd.StringDtype()) # type: ignore
        minted.block_height = minted.block_height.astype('int')
        minted.block_day = pd.to_datetime(minted.block_day, utc=True)
        minted.minted_to_treasury_amount = minted.minted_to_treasury_amount.astype('float')
        minted.minted_amount = minted.minted_amount.astype('float')

        # force checksum addresses to lowercase
        minted.reserve = minted.reserve.str.lower()
        minted.atoken = minted.atoken.str.lower()
    
    context.add_output_metadata(
        {
            "num_records": len(minted),
            "preview": MetadataValue.md(minted.head().to_markdown()),
        }
    )

    return minted