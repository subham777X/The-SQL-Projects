--explore all the objects from the database--
select * from information_schema.tables

--explore all columns in the database--
select * from information_schema.columns
where table_schema = 'gold'

--explore all columns in the database where table dim.customers--
select * from information_schema.columns
where table_name = 'dim_customers'