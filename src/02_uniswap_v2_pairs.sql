BEGIN

CREATE TEMP FUNCTION
    PARSE_V2_CREATE_LOG(data STRING, topics ARRAY<STRING>)
    RETURNS STRUCT<`token0` STRING, `token1` STRING, `pair` STRING>
    LANGUAGE js AS """
    const parsedEvent = {
        "anonymous": false,
        "inputs": [{"indexed": true, "internalType": "address", "name": "token0", "type": "address"}, {"indexed": true, "internalType": "address", "name": "token1", "type": "address"}, {"indexed": false, "internalType": "address", "name": "pair", "type": "address"}, {"indexed": false, "internalType": "uint256", "name": "", "type": "uint256"}],
        "name": "PairCreated",
        "type": "event"
    }
    return abi.decodeEvent(parsedEvent, data, topics, false);
"""
    OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );

CREATE TABLE uniswap_v2_pairs
AS
(
    SELECT
        PARSE_V2_CREATE_LOG(logs.data, logs.topics).token0 AS token0,
        PARSE_V2_CREATE_LOG(logs.data, logs.topics).token1 AS token1,
        PARSE_V2_CREATE_LOG(logs.data, logs.topics).pair AS pair
    FROM `bigquery-public-data.crypto_ethereum.logs` AS logs
    WHERE address = '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f'
    AND topics[SAFE_OFFSET(0)] = '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9'
);

END;
