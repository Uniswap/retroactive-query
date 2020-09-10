BEGIN

CREATE TEMPORARY TABLE all_pairs AS (
  SELECT pair
  FROM uniswap_v1_pairs
  UNION ALL
  SELECT pair
  from uniswap_v2_pairs
);
-- returns all uniswap contracts
CREATE TABLE uniswap_contracts AS (
  SELECT pair as contract
  FROM all_pairs
  UNION
  DISTINCT
  SELECT '0x7a250d5630b4cf539739df2c5dacb4c659f2488d' as contract -- Uniswap v2 Router 02
  UNION
  DISTINCT
  SELECT '0xf164fc0ec4e93095b804a4795bbe1e041497b92a' as contract -- Uniswap v2 Router 01
  UNION
  DISTINCT
  SELECT '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f' as contract -- Uniswap v2 Factory
  UNION
  DISTINCT
  SELECT '0xc0a47dfe034b400b47bdad5fecda2621de6c4d95' as contract -- Uniswap v1 Factory
);

END;