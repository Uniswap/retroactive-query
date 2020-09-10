BEGIN
-- Code for parsing Sync events from Uniswap v2

CREATE TEMP FUNCTION
    PARSE_SYNC(data STRING, topics ARRAY<STRING>)
    RETURNS STRUCT<`reserve0` STRING, `reserve1` STRING>
    LANGUAGE js AS """
    var parsedEvent = {"anonymous": false, "inputs": [{"indexed": false, "internalType": "uint112", "name": "reserve0", "type": "uint112"}, {"indexed": false, "internalType": "uint112", "name": "reserve1", "type": "uint112"}], "name": "Sync", "type": "event"}
    return abi.decodeEvent(parsedEvent, data, topics, false);
"""
    OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );

CREATE TABLE uniswap_v2_syncs AS (
SELECT
    logs.block_timestamp AS block_timestamp
     ,logs.block_number AS block_number
     ,logs.log_index AS log_index
     ,PARSE_SYNC(logs.data, logs.topics).reserve0 AS reserve0
     ,PARSE_SYNC(logs.data, logs.topics).reserve1 AS reserve1
     ,token0
     ,token1
     -- for pool_eth, ignore pairs that don't contain WETH
     ,CAST(CASE WHEN token0='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN CAST(PARSE_SYNC(logs.data, logs.topics).reserve0 AS NUMERIC)
                WHEN token1='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' THEN CAST(PARSE_SYNC(logs.data, logs.topics).reserve1 AS NUMERIC)
                ELSE 0
    END AS NUMERIC) AS pool_eth
     ,address as pair
FROM `bigquery-public-data.crypto_ethereum.logs` AS logs
         JOIN uniswap_v2_pairs AS pairs ON logs.address = pairs.pair
    AND topics[SAFE_OFFSET(0)] = '0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1'
);

END;