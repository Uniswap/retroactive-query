BEGIN

    CREATE TABLE socks_redeemers AS (
        SELECT from_address as address,
          CAST(FLOOR(SUM(CAST(value AS FLOAT64)) / POWER(10, 18)) AS INT64) AS SOCKS_redeemed
        FROM `bigquery-public-data.crypto_ethereum.token_transfers`
        WHERE (
            token_address = "0x23b608675a2b2fb1890d3abbd85c5775c51691d5"
            AND to_address = "0x0000000000000000000000000000000000000000"
            AND block_timestamp < @cutoff_timestamp
          )
        GROUP BY from_address
        ORDER BY 2 DESC
    );

    CREATE TABLE socks_token_holders AS (
        WITH
            sends AS (
                SELECT from_address as address,
                       CAST(value AS NUMERIC) * -1 AS socks_delta
                FROM `bigquery-public-data.crypto_ethereum.token_transfers`
                WHERE (
                    token_address = "0x23b608675a2b2fb1890d3abbd85c5775c51691d5"
                    AND block_timestamp < @cutoff_timestamp
                )
            ),
            receives AS (
                SELECT to_address as address,
                       CAST(value AS NUMERIC) AS socks_delta
                FROM `bigquery-public-data.crypto_ethereum.token_transfers`
                WHERE (
                    token_address = "0x23b608675a2b2fb1890d3abbd85c5775c51691d5"
                    AND block_timestamp < @cutoff_timestamp
                )
            ),
            combined AS (
                SELECT address, socks_delta FROM sends
                UNION ALL
                SELECT address, socks_delta FROM receives
            )
            SELECT address,
                   (SUM(socks_delta) / 1e18) AS socks_balance
            FROM
                combined
            WHERE address NOT IN (SELECT contract FROM uniswap_contracts)
            GROUP BY address
            HAVING socks_balance >= 1
    );

    CREATE TABLE all_socks_users AS (
        SELECT DISTINCT address FROM socks_token_holders
        UNION DISTINCT
        SELECT DISTINCT address FROM socks_redeemers
    );

END;