/* grouping the data based on specific range */

--segment the products in the cost ranges and count how many products fall into each segment--

with product_segments as (
select 
product_key,
product_name,
cost,
case when cost<100 then 'Below 100'
 when cost between 100 and 500 then '100-500'
 when cost between 500 and 1000 then '500-1000'
 else 'Above 1000'
end cost_range 
from gold.dim_products )


select 
cost_range,
count(product_key) as total_products
from product_segments
group by 1
order by total_products desc

/* grouping the customers into the three segments based into their spending behaviour
  -VIP at least 12 months of history and spending more than 5000
  -REGULAR at least 12 months of history but spending 5000 or less
  -NEW having history less than 12 months 
  -find the total number of customers by each group   */

with customer_spending as (
select 
c.customer_key,
sum(f.sales_amount) as total_spending,
min(order_date) as first_order,
max(order_date) as last_order,
date_part('year' , age(max(order_date), min(order_date))) *12 +
date_part('months',age(max(order_date), min(order_date))) as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by 1 )

select 
customer_key,
total_spending,
lifespan,
case when lifespan >= 12 and total_spending > 5000 then 'VIP'
  when lifespan >= 12 and total_spending <= 5000 then 'Regular'
  else 'New'
  end customer_segment
 from customer_spending
order by 3 desc

 


select 
customer_key,
case when lifespan >= 12 and total_spending > 5000 then 'VIP'
  when lifespan >= 12 and total_spending <= 5000 then 'Regular'
  else 'New'
  end customer_segment
from customer_spending 


 select 
 customer_segment,
 count(customer_key) as total_customers
 from(
 select customer_key,
  case when lifespan >= 12 and total_spending > 5000 then 'VIP'
   when lifespan >= 12 and total_spending <= 5000 then 'Regular'
   else 'New'
   end customer_segment
 from customer_spending  
 ) t
group by customer_segment
order by total_customers desc

