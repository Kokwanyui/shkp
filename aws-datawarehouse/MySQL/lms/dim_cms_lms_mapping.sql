-- step 1: create new shop list
drop table if exists bi_dimension.tmp_shop;
Create temporary table bi_dimension.tmp_shop
Select t1.shop_id,
       t1.status as shop_status,
       CASE  WHEN  t1.status = 'A'  THEN 'Active'
			 WHEN  t1.status = 'I'  THEN 'Inactive'
			 WHEN  t1.status = 'V'  THEN 'Invalid' ELSE NULL END AS shop_status_detail,
       t2.property_id,
       CASE  WHEN (RIGHT(t1.shop_no, 1) = ',') THEN LEFT(t1.shop_no, (LENGTH(t1.shop_no) - 1)) ELSE t1.shop_no END AS unit_no,
       trim(t1.name_lang1) AS shop_name,
       t1.agreement_no1 as agreement_no
From shkpmalls_vip_bi.shop t1
left join bi_dimension.dim_mall_mapping t2 on t1.mall_id = t2.mall_id;

Select * from bi_dimension.tmp_shop;

-- step 2: inhereit past agreement no from previous mapping
Create table bi_dimension.dim_cms_lms_mapping_backup
Select * from bi_dimension.dim_cms_lms_mapping;

Create table bi_dimension.tmp_cms_lms_mapping
Select t1.*,
t3.LMS_agreement_no,
t2.Trade_Name as lms_trade_name,
t2.main_unit_no,
t2.property_id as lms_property_id,
t2.Standard_Brand_Name_Eng,
t2.Standard_Brand_Name_Tc,
t2.Standard_Group,
t2.Standard_Trade_Category,
t2.Standard_Trade_Mix,
t2.Standard_Sub_Trade_Mix,
t2.Updated_Month
from bi_dimension.tmp_shop t1
left join bi_dimension.dim_cms_lms_mapping_backup t3 on t1.shop_id = t3.shop_id
left join bi_dimension.dim_lms_mapping t2 on t3.LMS_agreement_no = t2.agreement_no;

-- step 3: add new agreement no and lms details with lms_mapping excel
Select * from bi_dimension.tmp_cms_lms_mapping
where LMS_agreement_no is null and shop_status ='A'
order by property_id;

-- step 4: check it any cms shop name not equal to lms shop name
Select *
from  bi_dimension.tmp_cms_lms_mapping
where shop_name != Standard_Brand_Name_Eng
and shop_status = 'A';

-- step 5: check overall again
Select * from bi_dimension.tmp_cms_lms_mapping
where shop_status = 'A'
order by Updated_Month desc;

-- step 6: cleaning data
set SQL_SAFE_UPDATES = 0;

Update bi_dimension.tmp_cms_lms_mapping
Set lms_trade_name = replace(lms_trade_name, '^', "'");

Update bi_dimension.tmp_cms_lms_mapping
Set Standard_Brand_Name_Eng = replace(Standard_Brand_Name_Eng, '^', "'");

Update bi_dimension.tmp_cms_lms_mapping
Set Standard_Brand_Name_Tc = replace(Standard_Brand_Name_Tc, '^', "'");

Update bi_dimension.tmp_cms_lms_mapping
Set Standard_Group = replace(Standard_Group, '^', "'");

Update bi_dimension.tmp_cms_lms_mapping
Set Standard_Trade_Category = replace(Standard_Trade_Category, '^', "'");

Update bi_dimension.tmp_cms_lms_mapping
Set Standard_Trade_Mix = replace(Standard_Trade_Mix, '^', "'");

Update bi_dimension.tmp_cms_lms_mapping
Set Standard_Sub_Trade_Mix = replace(Standard_Sub_Trade_Mix, '^', "'");

Update bi_dimension.tmp_cms_lms_mapping
Set Updated_Month = Date_format(current_date(), "%Y%m")
where Updated_month is null;

-- step 7: save as dim_cms_lms_mapping
drop table if exists  bi_dimension.dim_cms_lms_mapping;
create table bi_dimension.dim_cms_lms_mapping
Select * from bi_dimension.tmp_cms_lms_mapping;

-- step 8: drop tmp_cms_lms_mapping and backup if all fine
drop table if exists bi_dimension.tmp_cms_lms_mapping;
drop table if exists bi_dimension.dim_cms_lms_mapping_backup;




