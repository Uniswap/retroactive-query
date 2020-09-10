BEGIN

CREATE TABLE lp_query_part_2 AS (
    -- ALLOCATE REWARDS AMONG PAIRS

--- DETERMINE CUMULATIVE TOKEN ALLOCATION PER ETH, OVER TIME

    WITH with_weth_value_of_secondary_pairs_delta AS (SELECT *
                                                           ,CAST(weth_value_of_secondary_pairs AS NUMERIC) - coalesce(LAG(CAST(coalesce(weth_value_of_secondary_pairs, 0) AS NUMERIC)) OVER (PARTITION BY linking_token ORDER BY block_timestamp, log_index, delta_pool_shares), 0) AS weth_value_secondary_pairs_delta
                                                      FROM lp_query_part_1
    )

-- Compute reward per token on a per-block basis, to avoid BigQuery choking
       ,grouped_by_block as (
        SELECT block_timestamp, SUM(CAST(adjusted_delta_eth AS NUMERIC) + weth_value_secondary_pairs_delta) AS block_delta_eth_value
        FROM with_weth_value_of_secondary_pairs_delta
        GROUP BY block_timestamp
    )
       , with_some_aggregates as (
        SELECT *,
               coalesce(CAST(TIMESTAMP_DIFF(block_timestamp, LAG(block_timestamp, 1) OVER (ORDER BY block_timestamp), SECOND) AS NUMERIC), 0) AS delta_tokens_granted
                ,SUM(CAST(block_delta_eth_value AS NUMERIC)) OVER (ORDER BY block_timestamp) AS total_supply
        FROM grouped_by_block
    )
       ,with_reward_per_eth_delta AS (
        SELECT *
             -- lastTimeRewardApplicable().sub(lastUpdateTime).mul(rewardRate).mul(1e18).div(_totalSupply)
             ,delta_tokens_granted * POWER(10, 18) / LAG(total_supply, 1) OVER (ORDER BY block_timestamp) AS reward_per_eth_delta
        FROM with_some_aggregates)
       ,with_reward_per_eth AS (
        SELECT block_timestamp as block_timestamp
             --rewardPerTokenStored.add(...)
             ,SUM(reward_per_eth_delta) OVER (ORDER BY block_timestamp) AS reward_per_eth
        FROM with_reward_per_eth_delta
    )
       ,joined AS (
        SELECT with_weth_value_of_secondary_pairs_delta.*
             ,reward_per_eth
             ,SUM(adjusted_delta_eth) OVER (PARTITION BY pair ORDER BY with_weth_value_of_secondary_pairs_delta.block_timestamp, log_index, delta_pool_shares) AS pool_eth
        FROM with_weth_value_of_secondary_pairs_delta
                 LEFT JOIN with_reward_per_eth ON with_weth_value_of_secondary_pairs_delta.block_timestamp=with_reward_per_eth.block_timestamp
    )
       ,with_lagged_stuff AS (
        SELECT *
             ,LAG(pool_eth, 1) OVER (PARTITION BY pair ORDER BY block_timestamp, log_index, delta_pool_shares) as lagged_pool_eth
             ,LAG(token_in_secondary_pairs, 1) OVER (PARTITION BY linking_token ORDER BY block_timestamp, log_index, delta_pool_shares) as lagged_token_in_secondary_pairs
        FROM joined
    )


-- ALLOCATE TO EACH LINKING TOKEN'S SECONDARY PAIRS

       ,with_secondary_pairs_earned_delta AS (
        SELECT *
             -- lastTimeRewardApplicable().sub(lastUpdateTime).mul(rewardRate).mul(1e18).div(_totalSupply)
             ,(coalesce(LAG(weth_value_of_secondary_pairs, 1) OVER (PARTITION BY linking_token ORDER BY block_timestamp, log_index, delta_pool_shares), 0)) * (reward_per_eth - coalesce(LAG(reward_per_eth, 1) OVER (PARTITION BY linking_token ORDER BY block_timestamp, log_index, delta_pool_shares), 0)) AS secondary_pairs_earned_delta
        FROM with_lagged_stuff
    )
       ,with_secondary_pairs_earned AS (
        SELECT *
             --rewardPerTokenStored.add(...)
             ,SUM(secondary_pairs_earned_delta) OVER (PARTITION BY linking_token ORDER BY block_timestamp, log_index, delta_pool_shares) AS secondary_pairs_earned
        FROM with_secondary_pairs_earned_delta
    )

-- COMPUTE CUMULATIVE REWARD PER LINKING TOKEN IN SECONDARY PAIRS

       ,with_secondary_pairs_reward_per_token_delta AS (
        -- _balances[account].mul(rewardPerToken().sub(userRewardPerTokenPaid[account])).div(1e18).add(rewards[account]);
        SELECT *
             ,SUM(adjusted_linking_token_in_secondary_pairs_delta) OVER (PARTITION BY pair ORDER BY block_timestamp, log_index, delta_pool_shares) AS secondary_pair_reserve
             ,CASE WHEN lagged_token_in_secondary_pairs=0 THEN 0 ELSE (secondary_pairs_earned - LAG(secondary_pairs_earned, 1) OVER (PARTITION BY linking_token ORDER BY block_timestamp, log_index, delta_pool_shares)) / lagged_token_in_secondary_pairs END AS secondary_pairs_reward_per_token_delta
        FROM with_secondary_pairs_earned
    )

-- split query in two to prevent BigQuery from choking
       ,with_secondary_pairs_reward_per_token AS (
        SELECT *
             ,SUM(secondary_pairs_reward_per_token_delta) OVER (PARTITION BY linking_token ORDER BY block_timestamp, log_index, delta_pool_shares) AS secondary_pairs_reward_per_token
        FROM with_secondary_pairs_reward_per_token_delta
        WHERE linking_token != ''
        UNION ALL
        SELECT *
             ,0 AS secondary_pairs_reward_per_token
        FROM with_secondary_pairs_reward_per_token_delta
        WHERE linking_token = ''
    )

-- ALLOCATE TO PRIMARY PAIRS BASED ON THEIR BALANCE OF ETH, AND SECONDARY PAIRS BASED ON THEIR BALANCE OF LINKING TOKENS

       ,with_pool_earned_delta AS (
        SELECT *
             -- _balances[account].mul(rewardPerToken().sub(userRewardPerTokenPaid[account])).div(1e18).add(rewards[account]);
             ,CASE WHEN lagged_pool_eth != 0
                       -- WETH pair or v1 pair
                       THEN
                           (lagged_pool_eth * ((reward_per_eth - coalesce(LAG(reward_per_eth, 1) OVER (PARTITION BY pair ORDER BY block_timestamp, log_index, delta_pool_shares), 0)))) / POWER(10,18)
                   ELSE
                       -- non-WETH pair
                           ((coalesce(LAG(secondary_pair_reserve, 1) OVER (PARTITION BY pair ORDER BY block_timestamp, log_index, delta_pool_shares), 0)) * ((secondary_pairs_reward_per_token - coalesce(LAG(secondary_pairs_reward_per_token, 1) OVER (PARTITION BY pair ORDER BY block_timestamp, log_index, delta_pool_shares), 0)))) / POWER(10,18)
            END
            as pool_earned_delta
        FROM with_secondary_pairs_reward_per_token)

-- ALLOCATE TOKENS TO INDIVIDUAL ADDRESSES

       ,with_time_since_pair_update AS (
        SELECT block_timestamp
             ,log_index
             ,delta_pool_shares
             ,event
             ,pair
             ,address
             ,adjusted_delta_eth
             ,pool_earned_delta
             ,CAST(coalesce(TIMESTAMP_DIFF(block_timestamp, LAG(block_timestamp, 1) OVER (PARTITION BY pair ORDER BY block_timestamp, log_index, delta_pool_shares), SECOND),0) AS NUMERIC) AS time_since_pair_update
        FROM with_pool_earned_delta)
       ,with_pool_shares AS (SELECT
                                 *
--        ,SUM(pool_earned_delta) OVER (PARTITION BY pair ORDER BY block_timestamp, log_index) AS pool_earned
                                  ,SUM(CASE WHEN address="0x0000000000000000000000000000000000000000" THEN -1 * delta_pool_shares ELSE 0 END) OVER (PARTITION BY pair ORDER BY block_timestamp, log_index, delta_pool_shares) AS pool_share_supply
                                  ,SUM(delta_pool_shares) OVER (PARTITION BY pair, address ORDER BY block_timestamp, log_index, delta_pool_shares) AS address_pool_share_balance
                             FROM with_time_since_pair_update
    )
       ,with_lag_pool_share_supply AS (
        SELECT *
             ,coalesce(CAST(LAG(pool_share_supply, 1) OVER (PARTITION BY pair ORDER BY block_timestamp, log_index, delta_pool_shares) AS NUMERIC), 0) AS lag_pool_share_supply
        FROM with_pool_shares)
       ,with_reward_per_pool_share_delta AS (
        SELECT *
             -- the relevant reward code is: lastTimeRewardApplicable().sub(lastUpdateTime).mul(rewardRate).mul(1e18).div(_totalSupply)
             -- but we use slightly different logic
             -- we allocate the pool tokens earned since the last time evenly across the pool shares
             ,(CASE WHEN lag_pool_share_supply=0 THEN 0 ELSE pool_earned_delta * POWER(10,18) / lag_pool_share_supply END) AS reward_per_pool_share_delta
        FROM with_lag_pool_share_supply
    )
       ,with_reward_per_pool_share AS (
        SELECT *
             ,SUM(reward_per_pool_share_delta) OVER (PARTITION BY pair ORDER BY block_timestamp, log_index, delta_pool_shares) as reward_per_pool_share
        FROM with_reward_per_pool_share_delta
    )
       ,with_address_earned_delta as (
        SELECT *,
               CASE WHEN address="0x0000000000000000000000000000000000000000" THEN 0 ELSE ((coalesce(LAG(address_pool_share_balance, 1) OVER (PARTITION BY pair, address ORDER BY block_timestamp, log_index, delta_pool_shares), 0)) * ((reward_per_pool_share - coalesce(LAG(reward_per_pool_share, 1) OVER (PARTITION BY pair, address ORDER BY block_timestamp, log_index), 0)))) / POWER(10,18) END AS address_earned_delta
        FROM with_reward_per_pool_share)

    SELECT * FROM with_address_earned_delta
);

END;