---cleaning the data---

select 
nullif (sls_order_dt,0)
from bronze.crm_sales_details
where sls_order_dt <= 0 or length(sls_order_dt :: text) != 8



select 
nullif (sls_order_dt,0)
from bronze.crm_sales_details
where sls_order_dt <= 0 or length(sls_order_dt :: text) != 8

select * 
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

select distinct
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
  then sls_quantity * abs(sls_price)
  else sls_sales
 end as sls_sales ,
 case when sls_price is null or sls_price <= 0
  then sls_sales / coalesce(sls_quantity,0)
  else sls_price
  end as sls_price
from bronze.crm_sales_details

where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by 1,2,3



select
cid,
bdate,
gen
from silver.erp_cust_az12 limit 1000;



select * from
silver.crm_cust_info limit 5; 

select
replace(cid,'-',''),
case when trim(cntry) = 'DE' then 'Germany'
 when trim (cntry) in ('US','USA') then 'United States'
 when trim(cntry) = '' or cntry is null then 'n/a'
 else trim(cntry) 
end as cntry 
from bronze.erp_loc_a101

select distinct case when trim(cntry) = 'DE' then 'Germany'
 when trim (cntry) in ('US','USA') then 'United States'
 when trim(cntry) = '' or cntry is null then 'n/a'
 else trim(cntry) 
end as cntry 
from bronze.erp_loc_a101
order by cntry

select * from
silver.erp_loc_a101


select id,cat,subcat,maintenance
from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)




select * from
silver.erp_px_cat_g1v2 ;


call bronze.load_bronze();
call silver.load_silver();

/* working on the gold layer */
select              --joining the data ----
 ci.cst_id,
 ci.cst_key,
 ci.cst_firstname,
 ci.cst_lastname,
 ci.cst_martial_status,
 ci.cst_gndr,
 ci.cst_create_date,
 ca.bdate ,
 ca.gen ,
 la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on    ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on    ci.cst_key = la.cid




select cst_id , count(*) from(        ---- checking for the duplicates after the joining ---
select 
 ci.cst_id,
 ci.cst_key,
 ci.cst_firstname,
 ci.cst_lastname,
 ci.cst_martial_status,
 case when ci.cst_gndr != 'n/a' then ci.cst_gndr   --crm is the master data for gender---
 else coalesce (ca.gen, 'n/a')
 end as new_gen,
 ci.cst_create_date,
 ca.bdate ,
 la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on    ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on    ci.cst_key = la.cid
) group by 
cst_id having count(*) >1



select   distinct           --joining the data ----
 ci.cst_gndr,
 ca.gen 
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on    ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on    ci.cst_key = la.cid
order by 1,2


select   distinct           --getting the gender data correct ----
 ci.cst_gndr,
 ca.gen ,
 case when ci.cst_gndr != 'n/a' then ci.cst_gndr   --crm is the master data for gender---
 else coalesce (ca.gen, 'n/a')
 end as new_gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on    ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on    ci.cst_key = la.cid
order by 1,2


create view gold.dim_customers as 

select 
row_number() over (Order by cst_id) as customer_key, ----the naming convention---
 ci.cst_id as customer_id,   ---also inserting the surrogate key-----
 ci.cst_key as customer_number,
 ci.cst_firstname as first_name,
 ci.cst_lastname as last_name,
  la.cntry as country,
 ci.cst_martial_status as marital_status,
 case when ci.cst_gndr != 'n/a' then ci.cst_gndr   --crm is the master data for gender---
 else coalesce (ca.gen, 'n/a')
 end as gender,
 ca.bdate as birthdate,
 ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on    ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on    ci.cst_key = la.cid

select * from gold.dim_customers             -----till here all the customers information was dealt with---

select prd_key ,count(*) from(
select
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date,
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null
)
group by prd_key having count(*) >1   --- no duplicates--

create view gold.dim_products as 
select
row_number() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null

select * from gold.dim_products


---building the facts---
create view gold.fact_sales as
select 
sd.sls_ord_num as order_number,
pr.product_key ,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id


select * from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
left join gold.dim_products p
on p.product_key = f.product_key
where p.product_key is null