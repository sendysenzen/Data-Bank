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
    customer_nodes

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
GROUP BY 1


-- A.3 How many customers are allocated to each region?

SELECT 
    r.region_name, 
    COUNT(DISTINCT cn.customer_id) num_unique_customer
FROM 
    regions r
INNER JOIN customer_nodes cn
    ON r.region_id = cn.region_id
GROUP BY 1

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
WHERE row_num > 1
-- yes every customer_id never switches region, there is always ONE combination of customer_id & region_id, for every customer
-- so in this case we can do partition by custoemer_id. 
    
-- substract the start_date when they start new node with the start_date when they start the previous node, 
-- but the node_id must be different. 
    
SELECT *,
    (end_date - start_date) as diff_date,
    ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY start_date) row_num
FROM customer_nodes
ORDER BY customer_id

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
FROM cte_duration
    
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
GROUP BY region_id
 

-- B. Customer Transactions
-- B.1 What is the unique count and total amount for each transaction type?



-- B.2 What is the average total historical deposit counts and amounts for all customers?



-- B.3 For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?



-- B.4 What is the closing balance for each customer at the end of the month?






/* B.5 Comparing the closing balance of a customer’s first month and the closing balance from their second nth, what percentage of customers:
i. Have a negative first month balance?
ii. Have a positive first month balance?
iii. Increase their opening month’s positive closing balance by more than 5% in the following month?
iv. Reduce their opening month’s positive closing balance by more than 5% in the following month?
v. Move from a positive balance in the first month to a negative balance in the second month?
*/ 

