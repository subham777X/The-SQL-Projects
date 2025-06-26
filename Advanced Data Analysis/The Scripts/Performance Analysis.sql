---comparison of the current value to a target value---
/*analyze the yearly performance of products by comparing their sales to both the average
sales performance of the product and the previous year's sales */

---this one is comparing current sales with the average and prev year sales---

with yearly_product_sales as (
select 
extract(year from f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where f.order_date is not null
group by 1,2)

select 
order_year,
product_name,
current_sales,
round(avg(current_sales) over (partition by product_name)) avg_sales,
current_sales - round(avg(current_sales) over (partition by product_name)) as diff_avg,
case when current_sales - round(avg(current_sales) over (partition by product_name)) > 0 then 'Above Average'
  when current_sales - round(avg(current_sales) over (partition by product_name)) < 0 then 'Below Average'
 else 'Avg'
 end as average_change,
 lag(current_sales) over (partition by product_name order by order_year) py_sales,
 current_sales - lag(current_sales) over (partition by product_name order by order_year) prev_yr_diff,
 case when current_sales - lag(current_sales) over (partition by product_name order by order_year) > 0 then 'Increasing'
  when current_sales - lag(current_sales) over (partition by product_name order by order_year) < 0 then 'Decreasing'
  else 'No Change'
  end prev_yr_change
from yearly_product_sales
order by product_name , order_year