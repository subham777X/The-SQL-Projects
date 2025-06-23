drop table if exists bronze.crm_cust_info;
create table bronze.crm_cust_info(
cst_id int,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_martial_status varchar(50),
cst_gndr varchar(50),
cst_create_date date
);


drop table if exists bronze.crm_prd_info;
create table bronze.crm_prd_info
(
prd_id int,
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt date
);


drop table if exists bronze.crm_sales_details;
create table bronze.crm_sales_details
(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
);


drop table if exists bronze.erp_cust_az12;
create table bronze.erp_cust_az12
(
cid varchar(50),
bdate date ,
gen varchar(50)
);


drop table if exists bronze.erp_loc_a101;
create table bronze.erp_loc_a101
(
cid varchar(50),
cntry varchar(50)
);


drop table if exists bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2
(
id varchar(50),
cat varchar(50),
subcat varchar(50),
maintenance varchar(50)
);

drop procedure if exists bronze.load_bronze();
create procedure bronze.load_bronze()
language plpgsql
as $$
	begin
	
	truncate table bronze.crm_cust_info;
	copy bronze.crm_cust_info
	from 'C:\Users\Public\sql project data\cust_info.csv'
	with (
	format csv,
	header true,
	delimiter ','
	);
	perform * from bronze.crm_cust_info limit 10;
	perform count(*) from bronze.crm_cust_info;
	
	
	truncate table bronze.crm_prd_info;
	copy bronze.crm_prd_info
	from 'C:\Users\Public\sql project data\prd_info.csv'
	with (
	format csv,
	header true,
	delimiter ','
	);
	perform * from bronze.crm_prd_info limit 10;
	perform count(*) from bronze.crm_prd_info;
	
	
	truncate table bronze.crm_sales_details ;
	copy bronze.crm_sales_details
	from 'C:\Users\Public\sql project data\sales_details.csv'
	with(
	format csv,
	header true,
	delimiter ','
	);
	perform * from bronze.crm_sales_details limit 10 ;
	perform count(*) from bronze.crm_sales_details ;
	
	
	truncate table bronze.erp_px_cat_g1v2 ;
	copy bronze.erp_px_cat_g1v2
	from 'C:\Users\Public\sql project data\px_cat_g1v2.csv'
	with(
	format csv,
	header true,
	delimiter ','
	);
	perform * from bronze.erp_px_cat_g1v2;
	perform count (*) from bronze.erp_px_cat_g1v2;
	
	
	truncate table bronze.erp_cust_az12;
	copy bronze.erp_cust_az12 
	from 'C:\Users\Public\sql project data\cust_az12.csv'
	with (
	format csv,
	header true,
	delimiter ','
	);
	perform * from bronze.erp_cust_az12 limit 10;
	perform count(*) from bronze.erp_cust_az12;
	
	
	truncate table bronze.erp_loc_a101;
	copy bronze.erp_loc_a101
	from 'C:\Users\Public\sql project data\loc_a101.csv'
	with(
	format csv,
	header true,
	delimiter ','
	);
	perform * from bronze.erp_loc_a101;
	perform count(*) from bronze.erp_loc_a101;
	
	end;
$$;


--the silver schema script--


drop table if exists silver.crm_cust_info;
create table silver.crm_cust_info(
cst_id int,
cst_key varchar(50),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_martial_status varchar(50),
cst_gndr varchar(50),
cst_create_date date,
dwh_create_date timestamp default now()
);


drop table if exists silver.crm_prd_info;   ----this was modified later to have the cat_id column---
create table silver.crm_prd_info
(
prd_id int,
cat_id varchar(50),
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date timestamp default now()
);


drop table if exists silver.crm_sales_details;
create table silver.crm_sales_details
(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int,
sls_order_dt date,           -------data type was later changed from int to date--------
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date timestamp default now()
);


drop table if exists silver.erp_cust_az12;
create table silver.erp_cust_az12
(
cid varchar(50),
bdate date ,
gen text,                                   --------------gen data type was later changed-------
dwh_create_date timestamp default now()
);


drop table if exists silver.erp_loc_a101;
create table silver.erp_loc_a101
(
cid varchar(50),
cntry varchar(50),
dwh_create_date timestamp default now()
);


drop table if exists silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2
(
id varchar(50),
cat varchar(50),
subcat varchar(50),
maintenance varchar(50),
dwh_create_date timestamp default now()
);

----script to insert the data in the silver schema----
drop procedure if exists silver.load_silver();
create procedure silver.load_silver()
language plpgsql
as $$
begin

	truncate table silver.crm_cust_info;
	insert into silver.crm_cust_info(                                 -----crm cust info data insertion-----
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
	
	truncate table silver.crm_prd_info;
	insert into silver.crm_prd_info (                              ----crm_prd_info data insertion----
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)
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
	from bronze.crm_prd_info;
	
	
	truncate table silver.crm_sales_details;
	insert into silver.crm_sales_details(           ----this is to insert data into silver.crm_sales_details----
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
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
	from bronze.crm_sales_details;
	
	
	truncate table silver.erp_cust_az12 ;
	insert into silver.erp_cust_az12                ---to insert the data in silver.erp_cust_az12----
	(cid , bdate , gen
	)
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
	from bronze.erp_cust_az12;
	
	
	truncate table silver.erp_loc_a101; 
	insert into silver.erp_loc_a101                  ------inserting the data in the silver.erp_loc_a101------
	(
	cid,cntry
	)
	select
	replace(cid,'-',''),
	case when trim(cntry) = 'DE' then 'Germany'
	 when trim (cntry) in ('US','USA') then 'United States'
	 when trim(cntry) = '' or cntry is null then 'n/a'
	 else trim(cntry) 
	end as cntry 
	from bronze.erp_loc_a101;
	
	
	truncate table silver.erp_px_cat_g1v2;
	insert into silver.erp_px_cat_g1v2(               ---- inserting the data in the silver.erp_px_cat_g1v2--
	id,cat,subcat,maintenance
	)
	select * from
	bronze.erp_px_cat_g1v2 ;
	
	end ;
$$;

call silver.load_silver();     ------ calling the procedure------