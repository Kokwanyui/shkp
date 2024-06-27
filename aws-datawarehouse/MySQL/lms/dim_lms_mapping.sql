-- After Finish Excel: 
-- insert column AC with below insert
insert into bi_dimension.dim_lms_mapping
values;


-- Data Cleaning
set SQL_SAFE_UPDATES = 0;

Update bi_dimension.dim_lms_mapping
Set Trade_Category = replace(Trade_Category, '^', "'");

Update bi_dimension.dim_lms_mapping
Set Trade_Mix = replace(Trade_Mix, '^', "'");

Update bi_dimension.dim_lms_mapping
Set Sub_Trade_Mix = replace(Sub_Trade_Mix, '^', "'");

Update bi_dimension.dim_lms_mapping
Set Trade_Name = replace(Trade_Name, '^', "'");

Update bi_dimension.dim_lms_mapping
Set Tenant = replace(Tenant, '^', "'");

Update bi_dimension.dim_lms_mapping
Set Standard_Brand_Name_Eng = replace(Standard_Brand_Name_Eng, '^', "'");

Update bi_dimension.dim_lms_mapping
Set Standard_Brand_Name_Tc = replace(Standard_Brand_Name_Tc, '^', "'");

Update bi_dimension.dim_lms_mapping
Set Standard_Group = replace(Standard_Group, '^', "'");

Update bi_dimension.dim_lms_mapping
Set Standard_Trade_Category = replace(Standard_Trade_Category, '^', "'");

Update bi_dimension.dim_lms_mapping
Set Standard_Trade_Mix = replace(Standard_Trade_Mix, '^', "'");

Update bi_dimension.dim_lms_mapping
Set Standard_Sub_Trade_Mix = replace(Standard_Sub_Trade_Mix, '^', "'");