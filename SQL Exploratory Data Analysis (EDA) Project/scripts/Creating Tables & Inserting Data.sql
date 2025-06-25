--creating the schema 'gold'--
create schema gold ;

---creating tables---
drop table if exists gold.dim_customers;
create table gold.dim_customers(
customer_key int,
customer_id int,
customer_number varchar(50),
first_name varchar(50),
last_name varchar(50),
country varchar(50),
marital_status varchar(50),
gender varchar(50),
birthdate date,
create_date date
);


drop table if exists gold.dim_products;
create table gold.dim_products(
product_key int,
product_id int,
product_number varchar(50),
product_name varchar(50),
category_id varchar(50),
category varchar(50),
subcategory varchar(50),
maintenance varchar(50),
cost int ,
product_line varchar(50),
start_date date 
);


drop table if exists gold.fact_sales;
create table gold.fact_sales(
order_number varchar(50),
product_key int ,
customer_key int ,
order_date date ,
shipping_date date ,
due_date date ,
sales_amount int,
quantity int,
price int 
);

---inserting data from csv files into the tables---

truncate table gold.dim_customers;
copy gold.dim_customers
from 'C:/Users/Public/csv-files/gold.dim_customers.csv'
with (
format csv,
header true,
delimiter ','
);

truncate table gold.dim_products;
copy gold.dim_products
from 'C:/Users/Public/csv-files/gold.dim_products.csv'
with (
format csv,
header true,
delimiter ','
);

truncate table gold.fact_sales;
copy gold.fact_sales
from 'C:/Users/Public/csv-files/gold.fact_sales.csv'
with(
format csv,
header true,
delimiter ','
);
