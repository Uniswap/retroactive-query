BEGIN
    CREATE TABLE sanity_checks AS
    (
    SELECT "all earnings addresses are valid addresses" AS test_case,
    CAST((SELECT COUNT(1)
                      FROM all_earnings
                      WHERE NOT REGEXP_CONTAINS(address, "^0x[a-f0-9]{40}$")) AS STRING) AS test_value,
           (CASE
                WHEN (SELECT COUNT(1)
                      FROM all_earnings
                      WHERE NOT REGEXP_CONTAINS(address, "^0x[a-f0-9]{40}$")) > 0 THEN FALSE
                ELSE TRUE
               END)                                     AS passes
    UNION ALL
    SELECT "all earnings add up to exactly total reward" AS test_case,
        CAST((SELECT SUM(earnings)
                      FROM all_earnings) AS STRING) as test_value,
           (CASE
                WHEN (SELECT CAST(SUM(earnings) AS INT64)
                      FROM all_earnings) = 150000000 THEN TRUE
                ELSE FALSE
               END)                                 AS passes
    UNION ALL
    SELECT "there are exactly 85 socks redeemers" AS test_case,
        CAST((SELECT COUNT(1)
                      FROM socks_redeemers) AS STRING) as test_value,
           (CASE
                WHEN (SELECT COUNT(1)
                      FROM socks_redeemers) = 85 THEN TRUE
                ELSE FALSE
               END)                                 AS passes
    UNION ALL
    SELECT "there are exactly 151 socks holders" AS test_case,
        CAST((SELECT COUNT(1)
                      FROM socks_token_holders) AS STRING) as test_value,
           (CASE
                WHEN (SELECT COUNT(1)
                      FROM socks_token_holders) = 151 THEN TRUE
                ELSE FALSE
               END)                                 AS passes
    UNION ALL
    SELECT "there are exactly 220 distinct socks holders + redeemers" AS test_case,
        CAST((SELECT COUNT(1)
                      FROM all_socks_users) AS STRING) as test_value,
           (CASE
                WHEN (SELECT COUNT(1)
                      FROM all_socks_users) = 220 THEN TRUE
                ELSE FALSE
               END)                                 AS passes
    UNION ALL
    SELECT "there are exactly 251534 users" AS test_case,
        CAST((SELECT COUNT(distinct address) from user_query) AS STRING) as test_value,
           (CASE
                WHEN ((SELECT COUNT(distinct address) from user_query)) = 251534 THEN TRUE
                ELSE FALSE
               END)                                 AS passes
    UNION ALL
    SELECT "no one gets less than the user_reward in UNI" AS test_case,
            CAST((SELECT MIN(earnings) FROM all_earnings) AS STRING) as test_value,
            (SELECT (MIN(earnings) = @user_reward) FROM all_earnings)
                                                    AS passes
    UNION ALL
    SELECT "there are exactly 49191 liquidity providers" AS test_case,
        CAST((SELECT COUNT(distinct address) from liquidity_provider_query) AS STRING) as test_value,
           (CASE
                WHEN ((SELECT COUNT(distinct address) from liquidity_provider_query)) = 49191 THEN TRUE
                ELSE FALSE
               END)                                 AS passes
    UNION ALL
    SELECT "there are no uniswap contracts" AS test_case,
        CAST((SELECT COUNT(*) from all_earnings
              WHERE address NOT IN (SELECT DISTINCT address FROM uniswap_contracts)) AS STRING) as test_value,
           (CASE
                WHEN (SELECT COUNT(*) from all_earnings
              WHERE address NOT IN (SELECT DISTINCT address FROM uniswap_contracts)) = 0 THEN TRUE
                ELSE FALSE
               END)                                 AS passes
                                                    );
END;