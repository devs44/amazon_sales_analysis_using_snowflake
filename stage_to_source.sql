use schema source;


create or replace sequence in_sales_order_seq 
  start = 1 
  increment = 1 
comment='This is sequence for India sales order table';

create or replace sequence us_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for USA sales order table';

create or replace sequence fr_sales_order_seq 
  start = 1 
  increment = 1 
  comment='This is sequence for France sales order table';

CREATE OR REPLACE TRANSIENT TABLE in_sales_order (
    sales_order_key NUMBER(38,0),
    order_id VARCHAR,
    customer_name VARCHAR,
    mobile_key VARCHAR,
    order_quantity NUMBER(38,0),
    unit_price NUMBER(38,0),
    order_value NUMBER(38,0),  -- Corrected typo here
    promotion_code VARCHAR,
    final_order_amount NUMBER(10,2),
    tax_amount NUMBER(10,2),
    order_dt DATE,
    payment_status VARCHAR,
    shipping_status VARCHAR,
    payment_method VARCHAR,
    payment_provider VARCHAR,
    mobile VARCHAR,
    shipping_address VARCHAR,
    _metadata_file_name VARCHAR,
    _metadata_row_number NUMBER(38,0),
    _metadata_last_modified TIMESTAMP_NTZ(9)
);


-- US Sales Table in Source Schema (Parquet File)
create or replace transient table us_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_valaue number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_dt date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 phone varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);

-- France Sales Table in Source Schema (JSON File)
create or replace transient table fr_sales_order (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_valaue number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_dt date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 phone varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);


------
SELECT SALES_DWH.SOURCE.IN_SALES_ORDER_SEQ.NEXTVAL


SHOW SEQUENCES LIKE 'IN_SALES_ORDER_SEQ' IN SCHEMA SALES_DWH.SOURCE;


select * from  sales_dwh.source.in_sales_order;
select * from  sales_dwh.source.us_sales_order;

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE SALES_DWH.SOURCE.IN_SALES_ORDER TO ROLE SYSADMIN;

GRANT USAGE ON SCHEMA SALES_DWH.COMMON TO ROLE SYSADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA SALES_DWH.COMMON TO ROLE SYSADMIN;

GRANT USAGE ON FILE FORMAT SALES_DWH.COMMON.MY_CSV_FORMAT TO ROLE SYSADMIN;

SHOW SEQUENCES IN SCHEMA SOURCE;

GRANT USAGE ON SEQUENCE SALES_DWH.SOURCE.IN_SALES_ORDER_SEQ TO ROLE SYSADMIN;



--ingest in  sales
copy into sales_dwh.source.in_sales_order from ( 
            select 
            in_sales_order_seq.nextval, 
            t.$1::text as order_id, 
            t.$2::text as customer_name, 
            t.$3::text as mobile_key,
            t.$4::number as order_quantity, 
            t.$5::number as unit_price, 
            t.$6::number as order_valaue,  
            t.$7::text as promotion_code , 
            t.$8::number(10,2)  as final_order_amount,
            t.$9::number(10,2) as tax_amount,
            t.$10::date as order_dt,
            t.$11::text as payment_status,
            t.$12::text as shipping_status,
            t.$13::text as payment_method,
            t.$14::text as payment_provider,
            t.$15::text as mobile,
            t.$16::text as shipping_address,
            metadata$filename as stg_file_name,
            metadata$file_row_number as stg_row_numer,
            metadata$file_last_modified as stg_last_modified
            from 
            @SALES_DWH.SOURCE.my_internal_stages/order-20200101.csv 
            (                                                             
                file_format => 'sales_dwh.common.my_csv_format'           
            ) t  )  on_error = 'Continue';     
            
select count(*) from sales_dwh.source.in_sales_order;

//ingest in  us sales--parquet
copy into sales_dwh.source.us_sales_order                
            from                                    
            (                                       
                select                              
                us_sales_order_seq.nextval, 
                $1:"Order ID"::text as orde_id,   
                $1:"Customer Name"::text as customer_name,
                $1:"Mobile Model"::text as mobile_key,
                to_number($1:"Quantity") as quantity,
                to_number($1:"Price per Unit") as unit_price,
                to_decimal($1:"Total Price") as total_price,
                $1:"Promotion Code"::text as promotion_code,
                $1:"Order Amount"::number(10,2) as order_amount,
                to_decimal($1:"Tax") as tax,
                $1:"Order Date"::date as order_dt,
                $1:"Payment Status"::text as payment_status,
                $1:"Shipping Status"::text as shipping_status,
                $1:"Payment Method"::text as payment_method,
                $1:"Payment Provider"::text as payment_provider,
                $1:"Phone"::text as phone,
                $1:"Delivery Address"::text as shipping_address,
                metadata$filename as stg_file_name,
                metadata$file_row_number as stg_row_numer,
                metadata$file_last_modified as stg_last_modified
                from                                
                    @SALES_DWH.SOURCE.my_internal_stages/order-20200102.snappy.parquet
                    (file_format => sales_dwh.common.my_parquet_format)
                    ) on_error = continue



copy into sales_dwh.source.fr_sales_order                                
        from                                                    
        (                                                       
            select                                              
            sales_dwh.source.fr_sales_order_seq.nextval,         
            $1:"Order ID"::text as orde_id,                   
            $1:"Customer Name"::text as customer_name,          
            $1:"Mobile Model"::text as mobile_key,              
            to_number($1:"Quantity") as quantity,               
            to_number($1:"Price per Unit") as unit_price,       
            to_decimal($1:"Total Price") as total_price,        
            $1:"Promotion Code"::text as promotion_code,        
            $1:"Order Amount"::number(10,2) as order_amount,    
            to_decimal($1:"Tax") as tax,                        
            $1:"Order Date"::date as order_dt,                  
            $1:"Payment Status"::text as payment_status,        
            $1:"Shipping Status"::text as shipping_status,      
            $1:"Payment Method"::text as payment_method,        
            $1:"Payment Provider"::text as payment_provider,    
            $1:"Phone"::text as phone,                          
            $1:"Delivery Address"::text as shipping_address ,    
            metadata$filename as stg_file_name,
            metadata$file_row_number as stg_row_numer,
            metadata$file_last_modified as stg_last_modified
            from                                                
            @SALES_DWH.SOURCE.my_internal_stages/order-20200102.json
            (file_format => sales_dwh.common.my_json_format)
            ) on_error=continue