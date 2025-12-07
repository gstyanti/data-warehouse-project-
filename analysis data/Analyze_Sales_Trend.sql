

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

-- memperingkas kolom (menggabungkan kolom tahun dan bulan dari order date)
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
