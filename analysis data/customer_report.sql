/*
 ===============================================================================
 Customer Report
 ===============================================================================
 purpose: 
 	- This report consolidate key customers metrics and behaviors
 	
 Highlights:
 	1. Gather essential fields such as names, ages, and transaction details.
 	2. Segments customers into categories (VIP, Regular, New) and age groups.
 	3. Aggregates customer-level metrics:
 		- total orders
 		- total sales
 		- total quantity purchased
 		- total products
 		- lifespan (in months) 
 	4. Calculates valuable KPIs:
 		- recency (months since last order)
 		- average order value
 		- average monthly spend
================================================================================
 */
/*
 1) Base Query : Retrieve core columns from tables
 **/
create view gold.report_customers as

with base_query as (

select 
fs2.order_number, 
fs2.product_key,
fs2.order_date,
fs2.sales_amount,
fs2.quantity ,
dc.customer_key ,
dc.customer_number ,
concat(dc.first_name, ' ', dc.last_name) as customer_name  ,
extract(year from age(now(), dc.birthdate)) as age  
from gold.fact_sales fs2 
left join gold.dim_customers dc 
on dc.customer_key = fs2.customer_key 
where fs2.order_date is not null
),

customer_aggregation as (
/*----------------------------------------------------------------------
 2) Customer Aggregations: Summarizes key metrics at the customer level
 -----------------------------------------------------------------------
 */
select 
customer_key,
customer_number,
customer_name,
age,
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
sum (quantity) as total_quantity,
count (distinct product_key) as total_products,
max(order_date) as last_order_date,
(
	 extract(year from age(max(order_date), min(order_date))) * 12
	 +
	 extract(month from age(max(order_date), min(order_date)))
)
 as lifespan
from base_query
group by 
	customer_key, 
	customer_number, 
	customer_name, 
	age
)

select 
customer_key,
customer_number,
customer_name,
age,
case 
	when age < 20 then 'Under 20'
	when age between 20 and 29 then '20-29'
	when age between 30 and 39 then '30-39'
	when age between 40 and 49 then '40-49'
	else '50 and above'
end as age_group,
case 
	when lifespan >= 12 and total_sales > 5000 then 'VIP'
	when lifespan >= 12 and total_sales <= 5000 then 'Regular'
	else 'New'
end as customer_segment,
last_order_date,
extract(day from age(now(), last_order_date)) as recency,
total_orders,
total_sales,
total_quantity,
total_products,
lifespan,
-- Compute average order value (AVO)
CASE WHEN total_sales = 0 THEN 0
	 ELSE total_sales / total_orders
END AS avg_order_value,
-- Compute average monthly spend
CASE WHEN lifespan = 0 THEN total_sales
     ELSE total_sales / lifespan
END AS avg_monthly_spend
FROM customer_aggregation;

select 
customer_segment ,
count (customer_number) as total_customers,
sum(total_sales) total_sales 
from gold.report_customers rc
group by customer_segment  ;
