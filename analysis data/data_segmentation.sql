/*
===============================================================================
Data Segmentation Analysis
===============================================================================
Purpose:
    - To group data into meaningful categories for targeted insights.
    - For customer segmentation, product categorization, or regional analysis.

SQL Functions Used:
    - CASE: Defines custom segmentation logic.
    - GROUP BY: Groups data into segments.
===============================================================================
*/

/*Segment products into cost ranges and 
 count how many products fall into each segments*/


with product_segments as (
select 
product_key,
product_name,
cost ,
case when cost < 100 then 'below 100'
	when cost between 100 and 500 then '100-500'
	when cost between 500 and 1000 then '500-1000'
	else 'above 1000'
end cost_range
from gold.dim_products dp
)
select cost_range,
count(product_key) as total_products
from product_segments
group by cost_range
order by total_products desc
; 


/*
 Group customers into three segments based on their spending behavior:
 - VIP : Customers with at least 12 months of history and spending more than €5,000.
 - Regular : Customers with at least 12 months of history but spending €5,000 or less.
 - New : Customers with a lifespan less than 12 months.
 and find the  total number of customers by each group
 **/

with customer_spending as (
select 
dc.customer_key,
sum(fs2.sales_amount) as total_spending ,
min(order_date) as first_order,
max(order_date) as last_order,
(
	 extract(year from age(max(order_date), min(order_date))) * 12
	 +
	 extract(month from age(max(order_date), min(order_date)))
)
 as lifespan
from gold.fact_sales fs2 
left join gold.dim_customers dc 
on fs2.customer_key = dc.customer_key
group by dc.customer_key 
)
select 
customer_segment,
count(customer_key) as total_customers
from (
select 
customer_key,
case when lifespan >= 12 and total_spending > 5000 then 'VIP'
	 when lifespan >= 12 and total_spending <= 5000 then 'Regular'
	 else 'New'
end customer_segment
from customer_spending) t
group by customer_segment
order by total_customers desc;
