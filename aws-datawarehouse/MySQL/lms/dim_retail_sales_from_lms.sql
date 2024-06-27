/* step 1: check last lms records months and backup*/
-- check last lms records months
Select Distinct `month`, sum(lms_sales) as lms_sales, count(*) as sales_record
from bi_dimension.dim_retail_sales_from_lms
group by `month`;

-- create temporary backup table
Create table  bi_dimension.dim_retail_sales_from_lms_backup
Select * from bi_dimension.dim_retail_sales_from_lms;

/* step 2: delete last 2 months records, to refresh most updated data from excel*/
-- WARNING: MAKE SURE to-be-deleted data is in excel
-- to-be-deleted data:
Select distinct `month` from bi_dimension.dim_retail_sales_from_lms
order by `month` desc
limit 2;

-- delete last 2 months data
SET SQL_SAFE_UPDATES = 0;

Delete from bi_dimension.dim_retail_sales_from_lms
where `month` in (202204, 202205); -- insert last 2 months here (result of to-deleted data)

/* step 3: insert records from excel*/
-- insert data month by month
-- excel formula: ="('"&A6&"','"&B6&"',"&$C$5&","&C6&"),"
-- rmb to filter out null value in excel

insert into bi_dimension.dim_retail_sales_from_lms
values
;
-- example excel formula result ('APM-MILLENNIUM CITY 5','E04807',202204,834934.58),

/* step 4: check results */
Select Distinct `month`, sum(lms_sales) as lms_sales, count(*) as sales_record
from bi_dimension.dim_retail_sales_from_lms
group by `month`;

/* step 5: drop backup table if all fine*/
drop table bi_dimension.dim_retail_sales_from_lms_backup;


/* step 6: adjust mall for member contribution*/
update bi_dimension.dim_retail_sales_from_lms
set mall = 'PARK CENTRAL'
where agreement_no = 'E20242'

