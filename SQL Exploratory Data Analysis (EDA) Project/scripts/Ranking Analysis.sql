---which 5 products generate the highest revenue---
select p.product_name,
sum(f.sales_amount) total_revenue 
from gold.fact_sales f
left join gold.dim_products P
on p.product_key = f.product_key
group by 1
order by total_revenue desc
limit 5


---what are the 5 worst-performing in terms of sales---


select p.product_name,
sum(f.sales_amount) total_revenue 
from gold.fact_sales f
left join gold.dim_products P
on p.product_key = f.product_key
group by 1
order by total_revenue
limit 5


---what are the 5 subcategories generating highest revenue---
select p.subcategory,
sum(f.sales_amount) total_revenue 
from gold.fact_sales f
left join gold.dim_products P
on p.product_key = f.product_key
group by 1
order by total_revenue desc
limit 5

---5 products generating the highest revenue using the window function---
select * from(
select p.product_name,
sum(f.sales_amount) total_revenue ,
row_number()over(order by sum(f.sales_amount) desc) as rank_products
from gold.fact_sales f
left join gold.dim_products P
on p.product_key = f.product_key
group by 1)
limit 5


--top 10 customers generating the highest revenue--
select 
c.customer_key,
c.first_name,
c.last_name,
sum(f.sales_amount) as total_revenue
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
group by 
c.customer_key,
c.first_name,
c.last_name
order by 4 desc
limit 10

--the 3 customers with the fewest orders --
select 
c.customer_key,
c.first_name,
c.last_name,
count(distinct order_number) as total_orders
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
group by 
c.customer_key,
c.first_name,
c.last_name
order by 4 
limit 3
