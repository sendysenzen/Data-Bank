-- 4th case study from 8 weeks challenge
-- data input already done with details can be seen from the website


-- A. Customer Nodes Exploration
-- A.1 How many unique nodes are there on the Data Bank system?
-- the question is a bit tricky and you should relate to the nodes definition.
-- if the question is how many unique nodes id then it is straightforward count(distinct node_id)
-- but in this case we need to multiply the nodes id and the region id to get the unique nodes. 

SELECT 
    COUNT(DISTINCT node_id) * COUNT(DISTINCT region_id) total_unique_nodes 
FROM
    customer_nodes;

-- A.2 What is the number of nodes per region?
-- of course if you already answer the first question you can directly guess without performing this code
-- if the question directed to ask the number of unique nodes per region
SELECT 
    r.region_name, 
    COUNT(cn.region_id) number_of_nodes,
    COUNT(DISTINCT cn.node_id) num_unique_nodes
FROM 
    regions r
INNER JOIN customer_nodes cn
    ON r.region_id = cn.region_id
GROUP BY 1;


-- A.3 How many customers are allocated to each region?

SELECT 
    r.region_name, 
    COUNT(DISTINCT cn.customer_id) num_unique_customer
FROM 
    regions r
INNER JOIN customer_nodes cn
    ON r.region_id = cn.region_id
GROUP BY 1;

-- A.4 How many days on average are customers reallocated to a different node?
-- a customer considered reallocated to a different node
-- but did customer ever change a region? we need to make sure that a customer_id never switches region_id
WITH cte AS (
SELECT 
    customer_id,
    region_id,
    (customer_id || '-' || region_id) unique_key, 
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY region_id) row_num,
FROM customer_nodes
GROUP BY customer_id, region_id
order by 1,2
    )
SELECT 
    COUNT(*)
FROM cte
WHERE row_num > 1;
-- yes every customer_id never switches region, there is always ONE combination of customer_id & region_id, for every customer
-- so in this case we can do partition by custoemer_id. 
    
-- substract the start_date when they start new node with the start_date when they start the previous node, 
-- but the node_id must be different. 
    
SELECT *,
    (end_date - start_date) as diff_date,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY start_date) row_num
FROM customer_nodes
ORDER BY customer_id;

WITH cte AS ( 
SELECT *,
    (end_date - start_date) as diff_date,
    LEAD(node_id,1) OVER(PARTITION BY customer_id ORDER BY start_date) lead_node,
    LEAD(start_date,1) OVER(PARTITION BY customer_id ORDER BY start_date) lead_start_date,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY start_date) row_num
FROM customer_nodes
order by customer_id
    ), cte_duration AS (
SELECT
    customer_id,
    region_id,
    (lead_start_date - start_date) duration
FROM cte
WHERE lead_node <> node_id
    )
SELECT 
    ROUND(AVG(duration),0) average_to_change_node
FROM cte_duration;
    
-- A.5 What is the median, 80th and 95th percentile for this same reallocation days metric for each region?

WITH cte AS ( 
SELECT *,
    (end_date - start_date) as diff_date,
    LEAD(node_id,1) OVER(PARTITION BY customer_id ORDER BY start_date) lead_node,
    LEAD(start_date,1) OVER(PARTITION BY customer_id ORDER BY start_date) lead_start_date,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY start_date) row_num
FROM customer_nodes
order by customer_id
    ), cte_duration AS (
SELECT
    customer_id,
    region_id,
    (lead_start_date - start_date) duration
FROM cte
WHERE lead_node <> node_id
    )
SELECT 
    region_id,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration) as median,
    PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY duration) as pctil80,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration) as pctil95
FROM cte_duration
GROUP BY region_id;
 

-- B. Customer Transactions
-- B.1 What is the unique count and total amount for each transaction type?
SELECT 
    txn_type,
    count(txn_type) count_type,
    sum(txn_amount) total_amt
FROM
    customer_transactions
GROUP BY txn_type;

-- B.2 What is the average total historical deposit counts and amounts for all customers?

WITH cte AS (
SELECT
    customer_id,
    count(txn_type) count_deposit,
    sum(txn_amount) total_amt
FROM customer_transactions
WHERE txn_type = 'deposit'
GROUP BY customer_id
) 
SELECT 
    ROUND(AVG(count_deposit)) avg_count_deposit,
    ROUND(SUM(total_amt)/SUM(count_deposit)) avg_deposit
FROM cte;
    
    
-- B.3 For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
WITH cte AS (
SELECT 
    customer_id,
    DATE_PART('month', txn_date) month_num,
    TO_CHAR(txn_date,'Month') month_txn,
    SUM(CASE WHEN txn_type = 'deposit' THEN 1 ELSE 0 END) count_deposit, 
    SUM(CASE WHEN txn_type = 'purchase' THEN 1 ELSE 0 END) count_purchase, 
    SUM(CASE WHEN txn_type = 'withdrawal' THEN 1 ELSE 0 END) count_withdraw
FROM customer_transactions
GROUP BY 1,2,3
ORDER BY 1
)
SELECT 
    month_num,
    month_txn,
    COUNT(DISTINCT customer_id) customer_count
FROM cte
WHERE 
    (count_purchase + count_withdraw > 0) 
    AND count_deposit > 1
GROUP BY 1,2
ORDER BY 1;

-- B.4 What is the closing balance for each customer at the end of the month?
-- first, what is the range of the month in the transcation?
SELECT
    max(txn_date),
    min(txn_date)
FROM customer_transactions;
-- range is from January to April. so the data will need to show 4 rows for 4 months for each customers

WITH cte_txn_exists AS ( 
SELECT 
    customer_id,
    (DATE_TRUNC('month', txn_date) + INTERVAL '1 month - 1 day')::DATE month_end,
    SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE 0 END) deposit, 
    SUM(CASE WHEN txn_type = 'purchase' THEN txn_amount ELSE 0 END) purchase, 
    SUM(CASE WHEN txn_type = 'withdrawal' THEN txn_amount ELSE 0 END) withdrawal
FROM customer_transactions
GROUP BY 1,2
ORDER BY 1,2
)
, cte_add_rows AS (  -- create 2nd table to generate every month end for every customer
SELECT
    DISTINCT customer_id,
    ('2020-01-31'::DATE + (GENERATE_SERIES(0,3)*interval '1 month'))::DATE month_end
FROM cte_txn_exists
ORDER BY 1,2
)
, cte_join_rows AS ( 
SELECT 
    t2.customer_id,
    t2.month_end,
    (t1.deposit - t1.purchase - t1.withdrawal) monthly_txn
FROM cte_add_rows t2
LEFT JOIN cte_txn_exists t1
ON t2.customer_id = t1.customer_id AND t2.month_end = t1.month_end
ORDER BY 1,2
)
SELECT 
    customer_id,
    month_end,
    CASE WHEN monthly_txn IS NOT NULL THEN monthly_txn ELSE 0 END monthly_txn,
    SUM(monthly_txn) OVER (PARTITION BY customer_id ORDER BY month_end
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) closing_balance
FROM cte_join_rows  ;      


/* B.5 Comparing the closing balance of a customer’s first month and the closing balance from their second nth, what percentage of customers:
i. Have a negative first month balance?
ii. Have a positive first month balance?
iii. Increase their opening month’s positive closing balance by more than 5% in the following month?
iv. Reduce their opening month’s positive closing balance by more than 5% in the following month?
v. Move from a positive balance in the first month to a negative balance in the second month?
*/ 

-- use previous answer and create it as temp table so the cte is not too long.
-- added lag for previous month closing balance and previous month transaction

DROP TABLE IF EXISTS temp_closing_balance ;
CREATE TEMP TABLE temp_closing_balance AS
WITH cte_txn_exists AS ( 
SELECT 
    customer_id,
    (DATE_TRUNC('month', txn_date) + INTERVAL '1 month - 1 day')::DATE month_end,
    SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE 0 END) deposit, 
    SUM(CASE WHEN txn_type = 'purchase' THEN txn_amount ELSE 0 END) purchase, 
    SUM(CASE WHEN txn_type = 'withdrawal' THEN txn_amount ELSE 0 END) withdrawal
FROM customer_transactions
GROUP BY 1,2
ORDER BY 1,2
)
, cte_add_rows AS (  -- create 2nd table to generate every month end for every customer
SELECT
    DISTINCT customer_id,
    ('2020-01-31'::DATE + (GENERATE_SERIES(0,3)*INTERVAL '1 month'))::DATE month_end
FROM cte_txn_exists
ORDER BY 1,2
)
, cte_join_rows AS ( 
SELECT 
    t2.customer_id,
    t2.month_end,
    (t1.deposit - t1.purchase - t1.withdrawal) monthly_txn
FROM cte_add_rows t2
LEFT JOIN cte_txn_exists t1
ON t2.customer_id = t1.customer_id AND t2.month_end = t1.month_end
ORDER BY 1,2
)
, cte_aggregate AS (
SELECT 
    customer_id,
    month_end,
    CASE WHEN monthly_txn IS NOT NULL THEN monthly_txn ELSE 0 END monthly_txn,
    LAG(monthly_txn) OVER(PARTITION BY customer_id ORDER BY month_end) previous_txn,
    SUM(monthly_txn) OVER (PARTITION BY customer_id ORDER BY month_end
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) closing_balance,
    ROW_NUMBER() OVER(PARTITION BY customer_id) row_num
FROM cte_join_rows 
)
SELECT 
    *,
    LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY month_end) previous_CB
FROM cte_aggregate;

-- calculate requirements 

SELECT *
FROM temp_closing_balance

WITH cte_grouping AS (
SELECT
    COUNT(DISTINCT customer_id) count_customer, 
        -- i. Have a negative first month balance?
    SUM(CASE WHEN closing_balance < 0 AND row_num = 1 THEN 1 ELSE 0 END) negative_cases, 
        -- ii. Have a positive first month balance?
    SUM(CASE WHEN closing_balance > 0 AND row_num = 1 THEN 1 ELSE 0 END) positive_cases, 
        -- iii. Increase their opening month’s positive closing balance by more than 5% in the following month?
    SUM(CASE WHEN row_num = 2 AND previous_cb > 0 AND (previous_cb * 1.05)<closing_balance 
            THEN 1 ELSE 0 END ) increase_cb,
        -- iv. Reduce their opening month’s positive closing balance by more than 5% in the following month?
    SUM(CASE WHEN row_num = 2 AND previous_cb > 0 AND (previous_cb * 0.95)>closing_balance 
            THEN 1 ELSE 0 END ) decrease_cb,
        -- v. Move from a positive balance in the first month to a negative balance in the second month?
    SUM(CASE WHEN row_num = 2 AND previous_cb > 0 AND closing_balance<0 
            THEN 1 ELSE 0 END ) switch_to_negative
FROM temp_closing_balance
)
SELECT 
    (100*negative_cases / count_customer) pct_negative_first_month, 
    (100*positive_cases / count_customer) pct_positive_first_month, 
    (100*increase_cb/positive_cases) pct_increased_balance, 
    (100*decrease_cb/positive_cases) pct_decreased_balance, 
    (100*switch_to_negative/positive_cases) pct_negative_balance
FROM cte_grouping;


-- however this answer has note on question iii and iv: opening months should be emphasized as the first month. 
-- this is important

-- and actually question ii is the opposite of question i so you just can use find answer question 1 then
-- use the (1 - x) to get the percentage for question 2. 
-- pct_negative_first_month + pct_positive_first_month is not 1 because of rounding 
-- this is something that I need to look up further as this is quite annoying when I cast to Numeric or decimal, 
-- the number behind the decimal doesn't appear


