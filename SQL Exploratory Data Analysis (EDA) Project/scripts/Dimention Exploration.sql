--exploring all the countries our customers are from --
select distinct country from gold.dim_customers;

--exploring all the 'major divisions' categories and subcategory--
select distinct category,subcategory , product_name from gold.dim_products
order by 1,2,3;

