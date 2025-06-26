/* reporting the data 
 -consolidation of the key metrics and behaviours

 _.gathering essential fields such as names , ages and transaction details.
 _.segments customers into categories (vip , regular, new) and age groups.
 _.aggregates customer-level metrics:
  -total orders
  -total sales
  -total quantity purchased
  -total products
  -lifespan (in months) 

_. calculates valuable KPI
 -recenscy (months since last order)
 -average order value
 -average monthly spend  

 */

 /* base query*/

create view gold.report_customer as 
with base_query as (
select 
 f.order_number,
 f.product_key,
 f.order_date,
 f.sales_amount,
 f.quantity,
 c.customer_key,
 c.customer_number,
 concat(c.first_name,' ',c.last_name) customer_name,
 extract (year from age(current_date , c.birthdate)) age
from gold.fact_sales f
 left join gold.dim_customers c
on c.customer_key = f.customer_key 
where order_date is not null
)

, customer_aggregation as (      /* the second cte*/
select  
 customer_key,
 customer_number,
 customer_name,
 age,
 count(distinct order_number) as total_orders,
 sum(sales_amount) as total_sales,
 sum(quantity) as total_quantity,
 count(distinct product_key) as total_products,
 max(order_date) as last_order_date,
 extract('year' from age(max(order_date),min(order_date))) *12 +
 extract('month' from age(max(order_date),min(order_date))) as lifespan
from base_query
group by 1,2,3,4
)

select 
customer_key,
customer_number,
customer_name,
case when age < 20 then 'Under 20'
 when age between 20 and 29 then '20-29'
 when age between 30 and 39 then '30-39'
 when age between 40 and 49 then '40-49'
 when age between 50 and 59 then '50-59'
 else '60+'
 end as age_group,
case 
 when lifespan >= 12 and total_sales > 5000 then 'VIP'
 when lifespan >=12 and total_sales <= 5000 then 'Regular'
 else 'New'
 end as customer_segment,
 last_order_date,
 age(current_date , last_order_date) as recency,
 total_orders,
 total_sales,
 total_quantity,
  total_products,
  lifespan,
  case when total_orders = 0 then 0        ---computing the average order value---
  else total_sales / total_orders
  end as average_order_value,
  case when lifespan = 0 then total_sales   ----computing the average monthly spend----
  else total_sales / lifespan
  end as avg_monthly_spend
  
from customer_aggregation  

