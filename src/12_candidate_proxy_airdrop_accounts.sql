BEGIN -- returns all addresses that were included as part of the trace of a transaction that interacted with Uniswap, but were not included in initial airdrop

CREATE TABLE candidate_proxy_airdrop_accounts AS (
  SELECT DISTINCT from_address
    FROM `bigquery-public-data.crypto_ethereum.traces`
    WHERE
      transaction_hash in (
        SELECT
          transaction_hash
        FROM `bigquery-public-data.crypto_ethereum.traces`
        WHERE
          to_address IN (
           SELECT contract
           FROM uniswap_contracts
        )
        AND block_timestamp < @cutoff_timestamp
        AND call_type = 'call'
      )
    AND from_address NOT IN (SELECT address from all_earnings)
);

END;