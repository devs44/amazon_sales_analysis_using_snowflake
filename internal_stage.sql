use schema source;

create or replace stage my_internal_stages;


desc stage my_internal_stages;

list  @my_internal_stages;


GRANT READ ON STAGE my_internal_stages TO ROLE SYSADMIN;
GRANT WRITE ON STAGE my_internal_stages TO ROLE SYSADMIN;
