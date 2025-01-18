/* 
This query calculates a total amount of Ethereum (ETH) staked with Lido Protocol. It encompasses
both deposits and the protocol buffer.

Calculations details:
1) Period: November 1, 2020 - current date
2) ETH staked = Total ETH deposited - Total ETH principal withdrawn +- Lido Buffer
    2.1) Total ETH deposited - sum of all ETH sent from stETH contract, Lido Withdrawal Vault and
    Lido Staking Router to Beacon Deposit Contract
        - ETH sent from stETH contract to Beacon Deposit Contract - until Lido V2 (last tx 2023-05-12)
        - ETH sent from Lido Withdrawal Vault and Lido Staking Router to Beacon Deposit Contract - since V2 (first tx 2023-05-18)
    2.2) Total ETH principal withdrawn
       We need to take into account principal withdrawals and not rewards withdrawals.
        - If the amount > 32ETH we take 32 as a principal
        - If amount in (20, 32) we consider this all to be a principal (e.g. after slashing) - this is empirical constant
        - Otherwise we consider principal to be 0 (whole amount is rewards)
    2.3) Buffer data from query_2481449
 */
-- This CTE generates a sequence of dates from November 1, 2020, to the current date

with calendar AS (
  with day_seq AS(SELECT( sequence(cast('2020-11-01' AS DATE),cast(NOW() AS DATE), interval '1' day)) day )
    SELECT days.day
    FROM day_seq
    CROSS JOIN unnest(day) AS days(day)
)

 -- This CTE calculates Lido daily deposits
, lido_deposits_daily AS (
    
    SELECT date_trunc('day',block_time) AS time, SUM(cast(value AS DOUBLE))/1e18 AS lido_deposited
    FROM  ethereum.traces
    WHERE to = 0x00000000219ab540356cbb839cbe05303d7705fa -- Beacon Deposit Contract
      AND call_type = 'call'
      AND success = True 
      -- stETH contract, Lido Withdrawal Vault, Lido Staking Router
      AND "from" in (0xae7ab96520de3a18e5e111b5eaab095312d7fe84, 0xB9D7934878B5FB9610B3fE8A5e441e8fad7E293f, 0xFdDf38947aFB03C621C71b06C9C70bce73f12999)
    GROUP BY 1
    
)

 -- This CTE calculates Lido daily withdrawals
, lido_all_withdrawals_daily AS (
    SELECT block_time AS time,
    SUM(amount)/1e9 AS amount,
    SUM(CASE WHEN amount/1e9 BETWEEN 20 AND 32 THEN CAST(amount AS DOUBLE)/1e9 
        WHEN amount/1e9 > 32 THEN 32
        ELSE 0 END) AS withdrawn_principal
    FROM ethereum.withdrawals
    WHERE address = 0xB9D7934878B5FB9610B3fE8A5e441e8fad7E293f
    GROUP BY 1
)

-- This CTE calculates Lido daily principal withdrawals
, lido_principal_withdrawals_daily AS (
    SELECT 
    date_trunc('day',time) AS time,
    (-1) * SUM(withdrawn_principal) AS amount
    FROM lido_all_withdrawals_daily
    WHERE withdrawn_principal > 0
    GROUP BY 1
 )

-- This CTE retrieves data from query_2481449 ('Lido_buffer') to calculate the daily buffer amount for Lido
, lido_buffer_amounts_daily AS (
SELECT * FROM query_2481449 --Lido protocol buffer
) 

-- final query combines all CTEs to compute daily and cumulative amounts of ETH deposited and withdrawn, the daily buffer amount and total amount of ETH in the Lido protocol 
SELECT 
    calendar.day
    , COALESCE(lido_deposited,0) AS lido_deposited_daily
    , SUM(COALESCE(lido_deposited,0)) over (ORDER BY calendar.day) AS lido_deposited_cumu
    , COALESCE(eth_balance,0) AS lido_buffer
    , COALESCE(withdrawals.amount,0) AS lido_witdrawals_daily
    , SUM(COALESCE(withdrawals.amount,0)) over (ORDER BY calendar.day) AS lido_withdrawals_cumu
    , SUM(COALESCE(lido_deposited,0)) over (ORDER BY calendar.day) + COALESCE(eth_balance,0) + SUM(COALESCE(withdrawals.amount,0)) over (ORDER BY calendar.day) AS lido_amount
FROM calendar
LEFT JOIN lido_deposits_daily AS lido_amounts ON lido_amounts.time = calendar.day
LEFT JOIN lido_buffer_amounts_daily AS buffer_amounts ON buffer_amounts.time = calendar.day
LEFT JOIN lido_principal_withdrawals_daily AS withdrawals ON withdrawals.time = calendar.day
ORDER BY 1
