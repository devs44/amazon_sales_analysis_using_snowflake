--create user and virtual warehouse for snowpark ETL


--create a virtual warehouse
create warehouse snowpark_etl_wh
    with
    warehouse_size = 'medium'
    warehouse_type = 'standard'
    auto_suspend = 60
    auto_resume = true
    min_cluster_count = 1
    max_cluster_count = 1
    scaling_policy = 'standard';

--create snowpark user(it can be only created using accountadmin role)
user role accountadmin;

CREATE USER snowpark_user
    PASSWORD = 'Test@12$4'
    COMMENT =  'This is snowpark user'
    DEFAULT_ROLE = sysadmin
    DEFAULT_SECONDARY_ROLES = ('ALL')
    MUST_CHANGE_PASSWORD = false;


--GRANTS
grant role sysadmin to user snowpark_user;
grant usage on warehouse snowpark_etl_wh to role sysadmin;

SELECT current_account();

GRANT USAGE ON DATABASE SALES_DWH TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA SALES_DWH.SOURCE TO ROLE SYSADMIN;
GRANT CREATE STAGE ON SCHEMA SALES_DWH.SOURCE TO ROLE SYSADMIN;

CREATE STAGE IF NOT EXISTS SALES_DWH.SOURCE.MY_INTERNAL_STG;
GRANT USAGE ON STAGE MY_INTERNAL_STG TO ROLE SYSADMIN;


