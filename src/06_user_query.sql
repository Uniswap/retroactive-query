BEGIN

CREATE TEMPORARY TABLE all_pairs AS (
  SELECT pair
  FROM uniswap_v1_pairs
  UNION ALL
  SELECT pair
  from uniswap_v2_pairs
);
-- returns all addresses that interacted with uniswap
CREATE TABLE user_query AS (
  WITH tokens AS (
    SELECT token
    FROM uniswap_v1_pairs
    UNION
    DISTINCT
    SELECT token0 AS token
    FROM uniswap_v2_pairs
    UNION
    DISTINCT
    SELECT token1 AS token
    FROM uniswap_v2_pairs
  ),
  token_transfer_senders AS (
      SELECT DISTINCT from_address as address
      FROM `bigquery-public-data.crypto_ethereum.token_transfers`
      WHERE
        (token_address IN (SELECT token FROM tokens) OR token_address IN (SELECT pair FROM all_pairs))
        AND to_address IN (
            SELECT contract
            FROM uniswap_contracts
        )
        AND block_timestamp < @cutoff_timestamp
  ),
  uniswap_traces as (
    SELECT
        from_address as address
    FROM `bigquery-public-data.crypto_ethereum.traces`
    WHERE
      to_address IN (
         SELECT contract
         FROM uniswap_contracts
      )
      AND block_timestamp < @cutoff_timestamp
      AND call_type = 'call'
  )
  SELECT DISTINCT address
  FROM (
      SELECT DISTINCT address
      FROM uniswap_traces
      UNION
      DISTINCT
      SELECT DISTINCT address
      FROM token_transfer_senders
    )
  WHERE address NOT IN (
      SELECT contract
      FROM uniswap_contracts
  )
);

END;