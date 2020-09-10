BEGIN

    CREATE TEMP FUNCTION
        PARSE_V1_LOG(data STRING, topics ARRAY<STRING>)
        RETURNS STRUCT<`caller` STRING, `tokens` STRING, `eth` STRING, `event` STRING>
        LANGUAGE js AS """
switch (topics[ 0 ]) {
  case '0x7f4091b46c33e918a0f3aa42307641d17bb67029427a5369e54b353984238705':
    var parsedEvent = { 'name': 'EthPurchase',
      'inputs': [{ 'type': 'address', 'name': 'buyer', 'indexed': true }, {
        'type': 'uint256',
        'name': 'tokens_sold',
        'indexed': true
      }, { 'type': 'uint256', 'name': 'eth_bought', 'indexed': true }],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { caller: decoded.buyer, tokens: decoded.tokens_sold, eth: decoded.eth_bought, event: 'EthPurchase' };
  case '0xcd60aa75dea3072fbc07ae6d7d856b5dc5f4eee88854f5b4abf7b680ef8bc50f':
    var parsedEvent = { 'name': 'TokenPurchase',
      'inputs': [{ 'type': 'address', 'name': 'buyer', 'indexed': true }, {
        'type': 'uint256',
        'name': 'eth_sold',
        'indexed': true
      }, { 'type': 'uint256', 'name': 'tokens_bought', 'indexed': true }],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { caller: decoded.buyer, tokens: decoded.tokens_bought, eth: decoded.eth_sold, event: 'TokenPurchase' };
  case '0x06239653922ac7bea6aa2b19dc486b9361821d37712eb796adfd38d81de278ca':
    var parsedEvent = { 'name': 'AddLiquidity',
      'inputs': [{ 'type': 'address', 'name': 'provider', 'indexed': true }, {
        'type': 'uint256',
        'name': 'eth_amount',
        'indexed': true
      }, { 'type': 'uint256', 'name': 'token_amount', 'indexed': true }],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { caller: decoded.provider, tokens: decoded.token_amount, eth: decoded.eth_amount, event: 'AddLiquidity' };
  case '0x0fbf06c058b90cb038a618f8c2acbf6145f8b3570fd1fa56abb8f0f3f05b36e8':
    var parsedEvent = { 'name': 'RemoveLiquidity',
      'inputs': [{ 'type': 'address', 'name': 'provider', 'indexed': true }, {
        'type': 'uint256',
        'name': 'eth_amount',
        'indexed': true
      }, { 'type': 'uint256', 'name': 'token_amount', 'indexed': true }],
      'anonymous': false,
      'type': 'event'
    };
    decoded = abi.decodeEvent(parsedEvent, data, topics, false);
    return { caller: decoded.provider, tokens: decoded.token_amount, eth: decoded.eth_amount, event: 'RemoveLiquidity' };
  default:
    throw 'unexpected event';
}
"""
        OPTIONS
  ( library="https://storage.googleapis.com/ethlab-183014.appspot.com/ethjs-abi.js" );

    CREATE TABLE
        -- Get non-transfer Uniswap v1 events
        parsed_v1_logs AS
    (
    SELECT logs.block_timestamp                 AS block_timestamp
         , logs.block_number                    AS block_number
         , logs.transaction_hash                AS transaction_hash
         , logs.log_index                       AS log_index
         , uniswap_v1_pairs.pair                AS pair
         , uniswap_v1_pairs.token               AS token
         , PARSE_V1_LOG(logs.data, logs.topics) AS parsed
    FROM `bigquery-public-data.crypto_ethereum.logs` AS logs
             JOIN uniswap_v1_pairs ON logs.address = uniswap_v1_pairs.pair
    WHERE topics[SAFE_OFFSET(0)] IN
          ('0x7f4091b46c33e918a0f3aa42307641d17bb67029427a5369e54b353984238705',
           '0xcd60aa75dea3072fbc07ae6d7d856b5dc5f4eee88854f5b4abf7b680ef8bc50f',
           '0x06239653922ac7bea6aa2b19dc486b9361821d37712eb796adfd38d81de278ca',
           '0x0fbf06c058b90cb038a618f8c2acbf6145f8b3570fd1fa56abb8f0f3f05b36e8')
            -- AND NOT exchange='0x009211344ee05ff3f69d9aadf0d3a0ab099c5363' -- avoid overflowing liquidity tokens
        );

END;