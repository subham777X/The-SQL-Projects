--finding the date of the first and the last order--
--years--
select min(order_date) first_order_date,
max(order_date) last_order_date,
extract(year from age(max(order_date),min(order_date))) as order_range_years
from gold.fact_sales


--in months--
select min(order_date) first_order_date,
max(order_date) last_order_date,
extract(year from age(max(order_date),min(order_date))) * 12 +
extract(month from age(max(order_date),min(order_date))) as order_range_month
from gold.fact_sales

--find the youngest and the oldest customer--
select
min(birthdate) as oldest_birthdate,
extract(year from age(current_date,max(birthdate))) current_age_youngest,
max(birthdate) as youngest_birthdate
from gold.dim_customers

select
min(birthdate) as oldest_birthdate,
extract(year from age(current_date,min(birthdate))) current_age_oldest,
max(birthdate) as youngest_birthdate
from gold.dim_customers