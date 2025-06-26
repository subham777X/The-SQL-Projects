---aggregating the data progressively over time to understand whether business is growing or declining---

--total sales for month along with the running total--
 select
 order_date,
 total_sales,
 sum(total_sales) over (partition by order_date order by order_date ) as running_total_sales
 from
 (
 select 
 date_trunc('month' ,order_date) :: date as order_date ,
 sum(sales_amount) as total_sales
 from gold.fact_sales
 where order_date is not null
 group by 1
 order by 1)

 
 ---for cumulative value over the year---
 select
 order_date,
 total_sales,
 sum(total_sales) over ( order by order_date ) as running_total_sales
 from
 (
 select 
 date_trunc('year' ,order_date) :: date as order_date ,
 sum(sales_amount) as total_sales
 from gold.fact_sales
 where order_date is not null
 group by 1
 order by 1

---for cumulative avg sales---
  select
 order_date,
 total_sales,
 sum(total_sales) over ( order by order_date ) as running_total_sales,
 round(avg(total_sales) over ( order by order_date),2) as moving_average_price
 from
 (
 select 
 date_trunc('year' ,order_date) :: date as order_date ,
 sum(sales_amount) as total_sales
 from gold.fact_sales
 where order_date is not null
 group by 1
 order by 1)