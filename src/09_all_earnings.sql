BEGIN -- returns each address and how much UNI it should receive
    CREATE TABLE all_earnings AS (
      WITH
       all_users AS (
           SELECT DISTINCT address FROM user_query
           UNION DISTINCT
           SELECT DISTINCT address FROM liquidity_provider_query WHERE address != ''
       ),
       total_misc_earnings AS (
        SELECT
            (SELECT SUM(@socks_user_reward) FROM all_socks_users) + (SELECT SUM(@user_reward) FROM all_users)
        AS total
      ),
      normalized_lp_earnings AS (
        -- scales earnings by the allocated amount over the total from the query
        SELECT address, earnings * (
            (SELECT (@total_reward - total) FROM total_misc_earnings) /
            (SELECT (SUM(earnings)) FROM liquidity_provider_query)
        ) as earnings
        FROM
          liquidity_provider_query WHERE address != ''
      ),
      combined_earnings AS (
        SELECT address,
          earnings,
          "lp" as reason
        FROM normalized_lp_earnings
        UNION ALL
        SELECT address,
          @user_reward as earnings,
          "user" as reason
        FROM all_users
        UNION ALL
        SELECT address,
          @socks_user_reward as earnings,
          "socks" as reason
        FROM all_socks_users
      )
      SELECT address,
        -- TODO: multiply by 1e18 and cast to string
        SUM(earnings) AS earnings,
        STRING_AGG(reason) AS reasons
      FROM combined_earnings
      GROUP BY address
    );
END;