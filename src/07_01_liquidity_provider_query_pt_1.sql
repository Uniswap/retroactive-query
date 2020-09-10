BEGIN

-- Get Uniswap v1 pairs
CREATE TABLE lp_query_part_1 AS (
    WITH
-- Get the v1 pair's change in ETH balance for each event
        deltas AS (SELECT
                       parsed_logs.block_timestamp as block_timestamp
                        ,parsed_logs.block_number as block_number
                        ,parsed_logs.log_index as log_index
                        ,CAST(0 AS NUMERIC) as delta_pool_shares
                        ,"" as address
       ,parsed_logs.parsed.event as event
       ,parsed_logs.pair as pair
       ,CASE WHEN parsed_logs.parsed.event="AddLiquidity" OR parsed_logs.parsed.event="TokenPurchase" THEN CAST(parsed_logs.parsed.eth AS NUMERIC)
        WHEN parsed_logs.parsed.event="RemoveLiquidity" OR parsed_logs.parsed.event="EthPurchase" THEN -1 * CAST(parsed_logs.parsed.eth AS NUMERIC)
    ELSE 0
END AS delta_eth
FROM parsed_v1_logs as parsed_logs
)
-- Get transfers of v1 pool shares (including the pool share component of mints and burns)
,v1_transfers AS
(SELECT
     *
 FROM `bigquery-public-data.crypto_ethereum.token_transfers` AS token_transfers
          JOIN uniswap_v1_pairs ON token_transfers.token_address = uniswap_v1_pairs.pair)

-- Take the union of all Uniswap v1 events
,uniswap_v1_events AS (SELECT
     *
FROM deltas
UNION ALL
SELECT
     block_timestamp
    ,block_number
    ,log_index
    -- one pair has liquidity tokens that overflow. This normalizes pool shares for just that pair
    ,-1 * CAST(CASE WHEN pair='0x009211344ee05ff3f69d9aadf0d3a0ab099c5363' THEN SUBSTR(`value`,0,LENGTH(`value`)-3) ELSE `value` END AS NUMERIC) AS delta_pool_shares
    ,from_address AS address
    ,"Transfer" as event
    ,pair
    ,0 as delta_eth
FROM v1_transfers
UNION ALL
SELECT
     block_timestamp
    ,block_number
    ,log_index
    -- one pair has liquidity tokens that overflow. This normalizes pool shares for just that pair
    ,CAST(CASE WHEN pair='0x009211344ee05ff3f69d9aadf0d3a0ab099c5363' THEN SUBSTR(`value`,0,LENGTH(`value`)-3) ELSE `value` END AS NUMERIC) AS delta_pool_shares
    ,to_address AS address
    ,"Transfer" as event
    ,pair
    ,0 as delta_eth
FROM v1_transfers)

,overflowing_pairs AS
(SELECT DISTINCT pair
FROM uniswap_v2_syncs
WHERE LENGTH(reserve0) > 29 OR LENGTH(reserve1) > 29)

,deltas_v2 AS
(
SELECT
   block_timestamp
  ,block_number
  ,log_index
  ,CAST(0 AS NUMERIC) as delta_pool_shares
  ,'' as address
  ,token0
  ,token1
  ,CAST(reserve0 AS NUMERIC) - CAST(coalesce(LAG(reserve0, 1) OVER (PARTITION BY pair ORDER BY block_number, log_index), '0') AS NUMERIC) AS reserve0_delta
  ,CAST(reserve1 AS NUMERIC) - CAST(coalesce(LAG(reserve1, 1) OVER (PARTITION BY pair ORDER BY block_number, log_index), '0') AS NUMERIC) AS reserve1_delta
  ,'Sync' as event
  ,pair
  ,pool_eth - coalesce(LAG(pool_eth, 1) OVER (PARTITION BY pair ORDER BY block_number, log_index), 0) AS delta_eth
FROM uniswap_v2_syncs
WHERE pair NOT IN (SELECT pair FROM overflowing_pairs)
)

-- Get transfers of v2 pool shares (including the pool share component of mints and burns)
,v2_transfers AS
(SELECT
     *
FROM `bigquery-public-data.crypto_ethereum.token_transfers` AS token_transfers
JOIN uniswap_v2_pairs ON token_transfers.token_address = uniswap_v2_pairs.pair)


-- Take the union of all v2 events
,uniswap_v2_events AS (
SELECT *
FROM deltas_v2
UNION ALL
SELECT
     block_timestamp
    ,block_number
    ,log_index
    ,-1 * CAST(`value` AS NUMERIC) AS delta_pool_shares
    ,from_address AS address
    ,token0
    ,token1
    ,0 as reserve0_delta
    ,0 as reserve1_delta
    ,"Transfer" as event
    ,pair
    ,0 as delta_eth
FROM v2_transfers
UNION ALL
SELECT
     block_timestamp
    ,block_number
    ,log_index
    ,CAST(`value` AS NUMERIC) AS delta_pool_shares
    ,to_address AS address
    ,token0
    ,token1
    ,0 as reserve0_delta
    ,0 as reserve1_delta
    ,"Transfer" as event
    ,pair
    ,0 as delta_eth
FROM v2_transfers
)

-- MERGE v1 AND v2 EVENTS

, all_events AS (
SELECT block_timestamp, log_index, delta_pool_shares, event
      ,0 as reserve0_delta -- not needed for v1 events
      ,0 as reserve1_delta -- not needed for v1 events
      ,'' as token0 -- not needed for v1 events
      ,'' as token1 -- not needed for v1 events
      ,address, pair, delta_eth
FROM uniswap_v1_events
WHERE block_timestamp < @cutoff_timestamp
UNION ALL
SELECT block_timestamp, log_index, delta_pool_shares, event, reserve0_delta, reserve1_delta, token0, token1, address, pair, delta_eth
FROM uniswap_v2_events
WHERE block_timestamp < @cutoff_timestamp
)
,maxes as (
SELECT MAX(block_timestamp) AS block_timestamp,
       1000000000 as log_index
FROM all_events
),positions AS (
SELECT DISTINCT pair, token0, token1, address FROM all_events
)
,with_end_dates AS (SELECT *
FROM all_events
UNION ALL
SELECT (SELECT block_timestamp from maxes) as block_timestamp
      ,(SELECT log_index from maxes) + 1 as log_index
      ,0 as delta_pool_shares
      ,'End' as event
      ,0 as reserve0_delta
      ,0 as reserve1_delta
      ,token0
      ,token1
      ,address
      ,pair
      ,0 as delta_eth,
FROM positions
)
, with_weth_deltas AS (
  SELECT *
        ,CASE WHEN token0='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN reserve1_delta
              WHEN token1='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN reserve0_delta
              ELSE 0 END AS weth_pair_other_token_delta
        ,CASE WHEN token0='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN token1
              ELSE token0
         END AS token0_or_other_token
        ,CASE WHEN token1='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN token0
              ELSE token1
         END AS token1_or_other_token
FROM with_end_dates
)
, with_aggregates AS (
SELECT *
      ,SUM(delta_eth) OVER (PARTITION BY token0_or_other_token ORDER BY block_timestamp, log_index) AS weth_pair_weth_balance_token0
      ,SUM(weth_pair_other_token_delta) OVER (PARTITION BY token0_or_other_token ORDER BY block_timestamp, log_index) AS weth_pair_other_token_balance_token0
      ,SUM(delta_eth) OVER (PARTITION BY token1_or_other_token ORDER BY block_timestamp, log_index) AS weth_pair_weth_balance_token1
      ,SUM(weth_pair_other_token_delta) OVER (PARTITION BY token1_or_other_token ORDER BY block_timestamp, log_index) AS weth_pair_other_token_balance_token1
      ,(token0='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' OR token1='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') AS is_weth_pair
FROM with_weth_deltas
)
, with_index AS (
SELECT *
      ,CASE WHEN is_weth_pair THEN CASE WHEN token0='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN 1
                                        ELSE 0 END
            WHEN COUNT(CASE WHEN weth_pair_weth_balance_token0 >= weth_pair_weth_balance_token1 THEN 1 END) OVER (PARTITION BY pair) > COUNT(CASE WHEN weth_pair_weth_balance_token0 < weth_pair_weth_balance_token1 THEN 1 END) OVER (PARTITION BY pair) THEN 0
            ELSE 1 END AS linking_token_index
FROM with_aggregates
)
, with_more_deltas AS (SELECT *
       ,CASE WHEN linking_token_index=0 THEN token0
             ELSE token1 END AS linking_token
       ,CASE WHEN is_weth_pair THEN 0 WHEN linking_token_index=0 THEN reserve0_delta ELSE reserve1_delta END AS linking_token_in_secondary_pairs_delta
       ,CASE WHEN NOT is_weth_pair THEN 0
             WHEN linking_token_index=0 THEN reserve1_delta
             ELSE reserve0_delta
        END AS weth_in_weth_pair_delta
       ,CASE WHEN NOT is_weth_pair THEN 0
             WHEN linking_token_index=0 THEN reserve0_delta
             ELSE reserve1_delta
        END AS linking_token_in_weth_pair_delta
FROM with_index
)
,with_total_supplies AS (SELECT *
      ,SUM(CASE WHEN address='0x0000000000000000000000000000000000000000' THEN -delta_pool_shares ELSE 0 END) OVER (PARTITION BY pair ORDER BY block_timestamp, log_index, delta_pool_shares) as total_supply
      ,SUM(delta_eth) OVER (PARTITION BY pair ORDER BY block_timestamp, log_index, delta_pool_shares) as eth_balance
      ,SUM(linking_token_in_secondary_pairs_delta) OVER (PARTITION BY pair ORDER BY block_timestamp, log_index, delta_pool_shares) as linking_token_balance
FROM with_more_deltas
)
, with_zeroed AS (
SELECT *
      ,CASE WHEN total_supply=0 THEN 0 ELSE eth_balance END AS adjusted_eth_balance
      ,CASE WHEN total_supply=0 THEN 0 ELSE linking_token_balance END AS adjusted_linking_token_balance
FROM with_total_supplies
)
, with_adjusted_deltas AS (
SELECT *
      ,adjusted_eth_balance - coalesce(LAG(adjusted_eth_balance, 1) OVER (PARTITION BY pair ORDER BY block_timestamp, log_index, delta_pool_shares)) AS adjusted_delta_eth
      ,adjusted_linking_token_balance - coalesce(LAG(adjusted_linking_token_balance, 1) OVER (PARTITION BY pair ORDER BY block_timestamp, log_index, delta_pool_shares)) AS adjusted_linking_token_in_secondary_pairs_delta
FROM with_zeroed
)
, with_more_aggregates AS (SELECT *
      ,SUM(adjusted_linking_token_in_secondary_pairs_delta) OVER (PARTITION BY linking_token ORDER BY block_timestamp, log_index) AS token_in_secondary_pairs
      ,SUM(weth_in_weth_pair_delta) OVER (PARTITION BY linking_token ORDER BY block_timestamp, log_index, delta_pool_shares) AS weth_in_primary_pair
      ,SUM(linking_token_in_weth_pair_delta) OVER (PARTITION BY linking_token ORDER BY block_timestamp, log_index, delta_pool_shares) AS token_in_primary_pair
FROM with_adjusted_deltas),
with_prices AS (
SELECT *
       -- FILTER OUT SECONDARY PAIRS WHEN PRIMARY PAIR HAS LOW LIQUIDITY
       -- THIS NUMBER SHOULD BE AT LEAST 1 ETH (10^18) TO AVOID SOME CASES THAT ACTUALLY OVERFLOW
       -- THE RIGHT NUMBER MAY BE MUCH HIGHER
      ,CASE WHEN weth_in_primary_pair < (POWER(10,18)) THEN 0
       -- AVOID DIVISION BY 0
            WHEN token_in_primary_pair=0 THEN 0
            ELSE (weth_in_primary_pair / token_in_primary_pair)
       END AS price
FROM with_more_aggregates
)
SELECT *
     ,token_in_secondary_pairs * price AS weth_value_of_secondary_pairs
FROM with_prices

    );

END;