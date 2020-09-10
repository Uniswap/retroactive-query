BEGIN
----- Helper functions


---- Code for parsing other staking contract events
CREATE TEMP FUNCTION
  PARSE_STAKED_LOG(data STRING, topics ARRAY<STRING>)
  RETURNS STRUCT<`user` STRING, `amount` STRING>
  LANGUAGE js AS """
    var parsedEvent = {"anonymous": false, "inputs": [{"indexed": true, "name": "user", "type": "address"}, {"indexed": false, "name": "amount", "type": "uint256"}], "name": "Staked", "type": "event"}
    return abi.decodeEvent(parsedEvent, data, topics, false);
"""
OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );

CREATE TEMP FUNCTION
  PARSE_WITHDRAWN_LOG(data STRING, topics ARRAY<STRING>)
  RETURNS STRUCT<`user` STRING, `amount` STRING>
  LANGUAGE js AS """
    var parsedEvent = {"anonymous": false, "inputs": [{"indexed": true, "name": "user", "type": "address"}, {"indexed": false, "name": "amount", "type": "uint256"}], "name": "Withdrawn", "type": "event"}
    return abi.decodeEvent(parsedEvent, data, topics, false);
"""
OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );

CREATE TEMP FUNCTION
  PARSE_GEYSER_STAKED_LOG(data STRING, topics ARRAY<STRING>)
  RETURNS STRUCT<`user` STRING, `amount` STRING, `total` STRING, `data` STRING>
  LANGUAGE js AS """
    var parsedEvent = {"anonymous": false, "inputs": [{"indexed": true, "name": "user", "type": "address"}, {"indexed": false, "name": "amount", "type": "uint256"}, {"indexed": false, "name": "total", "type": "uint256"}, {"indexed": false, "name": "data", "type": "bytes"}], "name": "Staked", "type": "event"}
    return abi.decodeEvent(parsedEvent, data, topics, false);
"""
OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );

CREATE TEMP FUNCTION
  PARSE_GEYSER_UNSTAKED_LOG(data STRING, topics ARRAY<STRING>)
  RETURNS STRUCT<`user` STRING, `amount` STRING, `total` STRING, `data` STRING>
  LANGUAGE js AS """
    var parsedEvent = {"anonymous": false, "inputs": [{"indexed": true, "name": "user", "type": "address"}, {"indexed": false, "name": "amount", "type": "uint256"}, {"indexed": false, "name": "total", "type": "uint256"}, {"indexed": false, "name": "data", "type": "bytes"}], "name": "Unstaked", "type": "event"}
    return abi.decodeEvent(parsedEvent, data, topics, false);
"""
OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );

CREATE TABLE liquidity_provider_query AS (
-- ALLOCATE TOKENS AMONG "SUBADDRESSES" OF STAKING CONTRACTS

--- GET KNOWN STAKING EVENTS FIRED BY LIQUIDITY PROVIDER ADDRESSES

WITH staked_geyser_logs AS
(SELECT
    logs.block_timestamp AS block_timestamp
    ,logs.log_index AS log_index
    ,PARSE_GEYSER_STAKED_LOG(logs.data, logs.topics) AS parsed
    ,address
FROM `bigquery-public-data.crypto_ethereum.logs` AS logs
WHERE (address='0x0eef70ab0638a763acb5178dd3c62e49767fd940' OR address='0xd36132e0c1141b26e62733e018f12eb38a7b7678')
AND topics[SAFE_OFFSET(0)] = '0xc65e53b88159e7d2c0fc12a0600072e28ae53ff73b4c1715369c30f160935142'
)
, unstaked_geyser_logs AS
(SELECT
    logs.block_timestamp AS block_timestamp
    ,logs.log_index AS log_index
    ,PARSE_GEYSER_UNSTAKED_LOG(logs.data, logs.topics) AS parsed
    ,address
FROM `bigquery-public-data.crypto_ethereum.logs` AS logs
WHERE (address='0x0eef70ab0638a763acb5178dd3c62e49767fd940' OR address='0xd36132e0c1141b26e62733e018f12eb38a7b7678')
  AND topics[SAFE_OFFSET(0)] = '0xaf01bfc8475df280aca00b578c4a948e6d95700f0db8c13365240f7f973c8754'
)
,possible_staking_contracts AS 
(
SELECT DISTINCT address FROM lp_query_part_2 WHERE address != '0xc5142a066dfcabbd20ff8b581c5e82f523af21bc' -- contract with incompatible interface
)
,staked_logs AS
(SELECT
    logs.block_timestamp AS block_timestamp
    ,logs.block_number AS block_number
    ,logs.transaction_hash AS transaction_hash
    ,logs.log_index AS log_index
    ,PARSE_STAKED_LOG(logs.data, logs.topics) AS parsed
    ,address
FROM `bigquery-public-data.crypto_ethereum.logs` AS logs
WHERE address IN (SELECT address FROM possible_staking_contracts)
  AND (topics[SAFE_OFFSET(0)] = '0x9e71bc8eea02a63969f509818f2dafb9254532904319f9dbda79b67bd34a5f3d'
       OR topics[SAFE_OFFSET(0)] = '0x9f9e4044c5742cca66ca090b21552bac14645e68bad7a92364a9d9ff18111a1c')
)

,withdrawn_logs AS
(SELECT
    logs.block_timestamp AS block_timestamp
    ,logs.block_number AS block_number
    ,logs.transaction_hash AS transaction_hash
    ,logs.log_index AS log_index
    ,PARSE_WITHDRAWN_LOG(logs.data, logs.topics) AS parsed
    ,address
FROM `bigquery-public-data.crypto_ethereum.logs` AS logs
WHERE address IN (SELECT address FROM possible_staking_contracts)
  AND topics[SAFE_OFFSET(0)] = '0x7084f5476618d8e60b11ef0d7d3f06914655adb8793e28ff7f018d4c76d505d5'
)
,staking_contracts AS (SELECT
     block_timestamp
     ,log_index
     ,address as staking_contract
    ,parsed.user AS `user`
    ,CAST(parsed.amount AS NUMERIC) AS `amount`
FROM staked_logs
UNION ALL
SELECT
     block_timestamp
     ,log_index
     ,address as staking_contract
    ,parsed.user AS `user`
    ,-1 * CAST(parsed.amount AS NUMERIC) AS `amount`
FROM withdrawn_logs
UNION ALL
SELECT block_timestamp
      ,log_index
      ,CASE WHEN address='0x0eef70ab0638a763acb5178dd3c62e49767fd940' THEN '0xe0624ab7206b847713b03f17602836205593497c' ELSE '0xce12d91c92f6fabe640c49d5f61c6715b0f5c034' END as staking_contract -- token pool contracts
      ,parsed.user as user
      ,CAST(parsed.amount AS NUMERIC) as amount, 
FROM staked_geyser_logs
UNION ALL
SELECT block_timestamp
      ,log_index
      ,CASE WHEN address='0x0eef70ab0638a763acb5178dd3c62e49767fd940' THEN '0xe0624ab7206b847713b03f17602836205593497c' ELSE '0xce12d91c92f6fabe640c49d5f61c6715b0f5c034' END as staking_contract -- token pool contracts
      ,parsed.user as user
      ,-1 * CAST(parsed.amount AS NUMERIC) as amount, 
FROM unstaked_geyser_logs 
)
, stakings AS (
SELECT block_timestamp, log_index, staking_contract as address, 0 as address_earned_delta, user as subaddress, amount as subamount_delta
FROM staking_contracts
WHERE block_timestamp < @cutoff_timestamp
)

-- ALLOCATE STAKING CONTRACT ADDRESS EARNINGS ACROSS SUBADDRESSES

, address_events AS (
SELECT block_timestamp, log_index, address, CAST(address_earned_delta AS NUMERIC), '' as subaddress, 0 as subamount_delta
FROM lp_query_part_2
WHERE address IN (SELECT address FROM stakings)
AND block_timestamp < @cutoff_timestamp
)
,combined AS (SELECT * FROM stakings
UNION ALL
SELECT * FROM address_events)
,positions AS (SELECT DISTINCT address, subaddress FROM combined)
,with_end_dates AS (
SELECT *
FROM combined
UNION ALL
SELECT
    (SELECT MAX(block_timestamp) FROM combined) as block_timestamp, 
    (SELECT MAX(log_index) FROM combined) + 1 as log_index, 
    address, 0 as address_earned_delta, subaddress, 0 as subamount_delta
FROM positions
)
,with_subtotal_supply AS (
SELECT *
      ,SUM(subamount_delta) OVER (PARTITION BY address ORDER BY block_timestamp, log_index) AS subtotal_supply
FROM with_end_dates)
,with_lagged_supply AS (
SELECT *
      ,coalesce(LAG(subtotal_supply, 1) OVER (PARTITION BY address ORDER BY block_timestamp, log_index), 0) AS lagged_supply
FROM with_subtotal_supply
)
,with_reward_per_subtoken_delta AS (
SELECT *
       -- lastTimeRewardApplicable().sub(lastUpdateTime).mul(rewardRate).mul(1e18).div(_totalSupply)
      ,CASE WHEN lagged_supply=0 THEN 0 ELSE CAST(address_earned_delta * POWER(10, 24) AS NUMERIC) / lagged_supply END AS reward_per_subtoken_delta
FROM with_lagged_supply
)
,with_reward_per_subtoken AS (
SELECT *
       --rewardPerTokenStored.add(...)
      ,SUM(reward_per_subtoken_delta) OVER (PARTITION BY address ORDER BY block_timestamp, log_index) AS reward_per_subtoken
FROM with_reward_per_subtoken_delta
)
,with_balances AS (
SELECT *
      ,SUM(subamount_delta) OVER (PARTITION BY address, subaddress ORDER BY block_timestamp, log_index) AS balance
FROM with_reward_per_subtoken
)
,with_subaddress_earned_delta AS (
SELECT *
       -- _balances[account].mul(rewardPerToken().sub(userRewardPerTokenPaid[account])).div(1e18).add(rewards[account]);
      ,CAST(coalesce(LAG(balance, 1) OVER (PARTITION BY address, subaddress ORDER BY block_timestamp, log_index),0) * (reward_per_subtoken - coalesce(LAG(reward_per_subtoken, 1) OVER (PARTITION BY address, subaddress ORDER BY block_timestamp, log_index), 0)) AS NUMERIC) / CAST(POWER(10, 24) AS NUMERIC) AS subaddress_earned_delta
FROM with_balances
)

-- COMBINE SUBADDRESS EARNINGS WITH ADDRESS EARNINGS FROM OTHER ADDRESSES

,all_earnings AS (
SELECT address, SUM(address_earned_delta) as earnings
FROM lp_query_part_2
WHERE address NOT IN (SELECT address FROM with_subaddress_earned_delta) 
  AND address NOT IN (SELECT contract FROM uniswap_contracts)
GROUP BY address
UNION ALL
SELECT subaddress as address, SUM(subaddress_earned_delta) as earnings
FROM with_subaddress_earned_delta
GROUP BY subaddress
)
SELECT address, SUM(earnings) AS earnings FROM all_earnings
WHERE address != ''
GROUP BY address
ORDER BY 2 DESC

);

END;