--find the total sales--
select sum(sales_amount) as total_sales from gold.fact_sales;

--find how many items are sold--
select sum(quantity) as total_quantity from gold.fact_sales;

--find average selling price--
select avg(price) as avg_price from gold.fact_sales;

--find the total number of orders--
select count(distinct order_number) as total_orders from gold.fact_sales;
select * from gold.fact_sales;

--find the total numbers of products--
select count(product_name) as total_products from gold.dim_products
select count(distinct product_name) as total_products from gold.dim_products

--find the total number of customers--
select count(customer_key) as total_customers from gold.dim_customers;

--find the total numbers of customers that has placed an order--
select count(distinct customer_key  ) as total_customers from gold.fact_sales;


--generate a report that shows all keys metrics of the business--
select 'Total Sales' as measure_name , sum(sales_amount) as measure_value 
from gold.fact_sales
union all
select 'Total Quantity' as measure_name, sum(quantity) as measure_value
from gold.fact_sales
union all
select 'Average Price' , round(avg(price),2) from gold.fact_sales
union all
select 'Toal Nr.Orders' , count(distinct order_number) from gold.fact_sales
union all
select 'Total Nr.Products' , count(product_name) from gold.dim_products
union all 
select 'Total Nr.Customers' , count(customer_key) from gold.dim_customers