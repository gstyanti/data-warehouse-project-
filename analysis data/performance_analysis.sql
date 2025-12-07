/*
===============================================================================
Performance Analysis (Year-over-Year, Month-over-Month)
===============================================================================
Purpose:
    - To measure the performance of products, customers, or regions over time.
    - For benchmarking and identifying high-performing entities.
    - To track yearly trends and growth.

SQL Functions Used:
    - LAG(): Accesses data from previous rows.
    - AVG() OVER(): Computes average values within partitions.
    - CASE: Defines conditional logic for trend analysis.
===============================================================================
*/

/* Analyze the yearly performance of products by comparing their sales 
to both the average sales performance of the product and the previous year's sales */

-- analyse performance overtime [Trends]
select 
extract (year from order_date) as order_year,
extract (month from order_date) as order_month,
sum(sales_amount) as total_sales, 
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales fs2 
where order_date is  not null 
group by extract(year from order_date) , extract(month from order_date)
order by extract(year from order_date), extract(month from order_date) ;


select 
to_char(order_date, 'YYYY-Mon') as order_date , 
sum(sales_amount) as total_sales, 
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales fs2 
where order_date is  not null 
group by to_char(order_date, 'YYYY-Mon') 
order by to_char(order_date, 'YYYY-Mon');

-- 
