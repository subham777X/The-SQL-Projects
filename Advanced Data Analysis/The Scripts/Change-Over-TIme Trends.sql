---query to get changes over year giving us high-level overview insights---
 
select extract(year from order_date) as order_year,
sum(sales_amount) total_sales ,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by order_year
order by 1


---query to get changes over months giving us high-level overview insights---

select extract(month from order_date) as order_month,
sum(sales_amount) total_sales ,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by order_month
order by 1

---query to get changes over months along with the year giving us high-level overview insights---

select date_trunc('month', order_date)::date as order_month,
sum(sales_amount) total_sales ,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by order_month
order by 1