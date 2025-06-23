---- to check for the duplicates and get the data with no duplicates ----

select cst_id , count(*) 
from bronze.crm_cust_info
group by 1 having count(*) > 1 or cst_id is null;

select *
from 
(
select * , row_number() over (partition by cst_id order by cst_create_date desc)
as flag_last
from bronze.crm_cust_info 
)
where flag_last = 1 ;                        

----to get the data with no unwanted empty spaces ---- 

select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname) ;   --- this is to check the spaces in the firstname----

select cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname) ;   ---- this is to check the spaces in the lastname ----

select cst_gndr
from bronze.crm_cust_info
where cst_gndr != trim(cst_gndr) ;           ---- this is to check the spaces in the gndr ----


----for data standardization and consistency----
select distinct cst_gndr
from bronze.crm_cust_info ;

select distinct cst_martial_status 
from bronze.crm_cust_info ;

-----to query the cleaned data -----


select cst_id ,
cst_key,
trim(cst_firstname),
trim(cst_lastname),
case when upper(trim(cst_martial_status)) = 'S' then 'Single'
  when upper(trim(cst_martial_status)) = 'M' then 'Married'
else 'n/a' end  cst_martial_status,
 case when upper(trim(cst_gndr)) = 'F' then 'Female'
 when upper(trim(cst_gndr)) = 'M' then 'Male'
 else 'n/a' end cst_gndr,
cst_create_date
from 
(
select * , row_number() over (partition by cst_id order by cst_create_date desc)
as flag_last
from bronze.crm_cust_info 
)
where flag_last = 1 ;  



----inserting the data into the silver schema----



insert into silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_martial_status,
cst_gndr,
cst_create_date
)
select cst_id ,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case when upper(trim(cst_martial_status)) = 'S' then 'Single'
  when upper(trim(cst_martial_status)) = 'M' then 'Married'
else 'n/a' end  cst_martial_status,
 case when upper(trim(cst_gndr)) = 'F' then 'Female'
 when upper(trim(cst_gndr)) = 'M' then 'Male'
 else 'n/a' end cst_gndr,
cst_create_date
from 
(
select * , row_number() over (partition by cst_id order by cst_create_date desc)
as flag_last
from bronze.crm_cust_info 
)
where flag_last = 1 ;  

 -----to check the data for the silver -----

 select cst_id , count(*) 
from silver.crm_cust_info
group by 1 having count(*) > 1 or cst_id is null;

select cst_firstname
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname);

select distinct cst_gndr
from silver.crm_cust_info ;

-----getting the entire data -----
select * from silver.crm_cust_info;

-----data cleansing with prd_info----


----chceking the duplicates----


select prd_id , count(*) 
from bronze.crm_prd_info
group by 1 having count(*) > 1 or prd_id is null;

select *
from bronze.crm_prd_info limit 10;

----this is to get the cat_id ----
select
prd_id,
prd_key,
replace(substring(prd_key,1,5),'-','_') as cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info


----this to check the query----

select
prd_id,
prd_key,
replace(substring(prd_key,1,5),'-','_') as cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where replace(substring(prd_key,1,5),'-','_') not in (
select distinct id from bronze.erp_px_cat_g1v2)

----the other data query----

select
prd_id,
prd_key,
replace(substring(prd_key,1,5),'-','_') as cat_id,
substring(prd_key,7,length(prd_key)) as prd_key_mid,
prd_nm,
coalesce(prd_cost,0),
case when upper(trim(prd_line)) = 'M' then 'Mountain'
 when upper(trim(prd_line)) = 'R' then 'Road'
 when upper(trim(prd_line)) = 'S' then 'Other Sales'
 when upper(trim(prd_line)) = 'T' then 'Touring'
 else 'n/a'
 end prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where
substring(prd_key,7,length(prd_key)) not in (
select distinct id from bronze.erp_px_cat_g1v2)

select sls_prd_key from bronze.crm_sales_details


select prd_cost
from bronze.crm_prd_info
where prd_cost < 0;

select distinct *
from bronze.crm_prd_info
where prd_start_dt > prd_end_dt




----to alter the query for the corrected start date and end date----

select
prd_id,
replace(substring(prd_key,1,5),'-','_') as cat_id,
substring(prd_key,7,length(prd_key)) as prd_key,
prd_nm,
coalesce(prd_cost,0),
case when upper(trim(prd_line)) = 'M' then 'Mountain'
 when upper(trim(prd_line)) = 'R' then 'Road'
 when upper(trim(prd_line)) = 'S' then 'Other Sales'
 when upper(trim(prd_line)) = 'T' then 'Touring'
 else 'n/a'
 end prd_line,
prd_start_dt,
lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt
from bronze.crm_prd_info

----this is for the crm_sales_details ----

select 
sls_ord_num ,
sls_prd_key, 
sls_cust_id ,
case when sls_order_dt = 0 or length(sls_order_dt :: text) != 8 then null
 else to_date (sls_order_dt :: text, 'YYYYMMDD') 
 end as sls_order_dt,
case when sls_ship_dt = 0 or length(sls_ship_dt :: text) != 8 then null
 else to_date (sls_ship_dt :: text , 'YYYYMMDD')
 end as sls_ship_dt, 
case when sls_due_dt = 0 or length(sls_due_dt :: text) != 8 then null
 else to_date (sls_due_dt :: text , 'YYYYMMDD')
 end as sls_due_dt,
case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
  then sls_quantity * abs(sls_price)
  else sls_sales
 end as sls_sales , 
sls_quantity ,
 case when sls_price is null or sls_price <= 0
  then sls_sales / coalesce(sls_quantity,0)
  else sls_price
  end as sls_price
from bronze.crm_sales_details



select * from 
silver.crm_sales_details


select
 case when cid like 'NAS%' then substring(cid,4,length(cid))
 else cid
end as cid,
case when bdate > current_date then null
  else bdate
  end,
case  when upper(trim(gen)) in ('F','FEMALE') then 'Female'
      when upper(trim(gen)) in ('M', 'MALE')  then  'Male'
	  else 'n/a'
end gen	
from bronze.erp_cust_az12

where (case when cid like 'NAS%' then substring(cid,4,length(cid))
 else cid end ) not in (select distinct cst_key from silver.crm_cust_info);

select distinct
gen,
case  when upper(trim(gen)) in ('F','FEMALE') then 'Female'
      when upper(trim(gen)) in ('M', 'MALE')  then  'Male'
	  else 'n/a'
end gen	  
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > current_date;

select id,cat,subcat,maintenance
from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance) 

select distinct maintenance,subcat
from bronze.erp_px_cat_g1v2
