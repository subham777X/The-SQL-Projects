----part to whole analysis , how an individual category is performing as compared to overall----
/* categories the contribute most to the sales*/

with category_sales as (
select 
category,
sum(sales_amount) total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by category)

select
	category,
	total_sales,
	sum(total_sales) over () overall_sales,
	(round((total_sales ::numeric / sum(total_sales) over ():: numeric) * 100 ,2)) as percentage_of_total
	from category_sales
order by 2 desc


