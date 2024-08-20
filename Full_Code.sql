CREATE DATABASE IF NOT EXISTS GBANK_DB;
CREATE SCHEMA IF NOT EXISTS GBANK_DB.raw_schema;
CREATE SCHEMA IF NOT EXISTS GBANK_DB.staging_schema;
CREATE SCHEMA IF NOT EXISTS GBANK_DB.analytics_schema;
CREATE SCHEMA IF NOT EXISTS GBANK_DB.security_schema;

-- Create table on raw_schema transactions data
CREATE OR REPLACE TABLE GBank_DB.RAW_SCHEMA.transactions(
    transaction_id STRING,    
    customer_id STRING,    
    transaction_date TIMESTAMP_NTZ,    
    amount FLOAT,
    currency STRING,    
    transaction_type STRING,    
    channel STRING,    
    merchant_name STRING,    
    merchant_category STRING,    
    location_country STRING,    
    location_city STRING,    
    is_flagged BOOLEAN
);

-- Create table on raw_schema customer data
CREATE OR REPLACE TABLE GBANK_DB.RAW_SCHEMA.customers(
    customer_id STRING,    
    first_name STRING,    
    last_name STRING,    
    date_of_birth DATE,
    gender STRING,    
    email STRING,    
    phone_number STRING,    
    address STRING,    
    city STRING,    
    country STRING,
    occupation STRING,
    income_bracket STRING,
    customer_since DATE
);

-- Create table on raw_schema account data
CREATE OR REPLACE TABLE GBANK_DB.RAW_SCHEMA.accounts(
    account_id STRING,
    customer_id STRING,
    account_type STRING,
    account_status STRING,
    open_date DATE,
    current_balance FLOAT,
    currency STRING,
    credit_limit FLOAT
);
-- Create table on raw_schema credit bureau data
CREATE OR REPLACE TABLE GBANK_DB.RAW_SCHEMA.credits(
    customer_id STRING,
    credit_score INT,
    number_of_credit_accounts INT,
    total_credit_limit FLOAT,
    total_credit_used FLOAT,
    number_of_late_payments INT,
    bankruptcies INT
);
-- Create table on raw_schema watchlist data
CREATE OR REPLACE TABLE GBANK_DB.RAW_SCHEMA.watchlists(
    entity_id STRING,
    entity_name STRING,
    entity_type STRING,
    risk_category STRING,
    listed_date DATE,
    source STRING
);

create or replace storage integration S3_INTEG_RAW_CSV
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::730335494857:role/cap-gbank-raw'
storage_allowed_locations = ('s3://aws-gbank-rawdata/aws-gbank-csv/');

-- Describe integration object to fetch external_id and to be used in s3 Trust Partnership
desc integration S3_INTEG_RAW_CSV;
  
-- Create csv file format
create or replace file format GBANK_DB.RAW_SCHEMA.cap_csv_file_format
type = 'csv';

-- Create external stage object
create or replace stage GBANK_DB.RAW_SCHEMA.sta_gbank_s3_data
URL = 's3://aws-gbank-rawdata/aws-gbank-csv/'
STORAGE_INTEGRATION = S3_INTEG_RAW_CSV
file_format = GBANK_DB.RAW_SCHEMA.cap_csv_file_format;


list @sta_gbank_s3_data;


--Create pipe for transaction raw data
create or replace pipe transactions_pipe 
auto_ingest = true as
copy into transactions 
from @GBANK_DB.RAW_SCHEMA.sta_gbank_s3_data/Gbank_transactions.csv
      file_format = GBANK_DB.RAW_SCHEMA.cap_csv_file_format
      on_error = 'continue';

 --Create pipe for transaction raw data
create or replace pipe customers_pipe 
auto_ingest = true as
copy into customers 
from @GBANK_DB.RAW_SCHEMA.sta_gbank_s3_data/Gbank_customers.csv
      file_format = GBANK_DB.RAW_SCHEMA.cap_csv_file_format
      on_error = 'continue';

      --Create pipe for transaction raw data
create or replace pipe accounts_pipe 
auto_ingest = true as
copy into accounts 
from @GBANK_DB.RAW_SCHEMA.sta_gbank_s3_data/Gbank_accounts.csv
      file_format = GBANK_DB.RAW_SCHEMA.cap_csv_file_format
      on_error = 'continue';


--Create pipe for transaction raw data
create or replace pipe credits_pipe 
auto_ingest = true as
copy into credits 
from @GBANK_DB.RAW_SCHEMA.sta_gbank_s3_data/Gbank_credits.csv
      file_format = GBANK_DB.RAW_SCHEMA.cap_csv_file_format
      on_error = 'continue';


--Create pipe for transaction raw data
create or replace pipe watchlists_pipe 
auto_ingest = true as
copy into watchlists 
from @GBANK_DB.RAW_SCHEMA.sta_gbank_s3_data/Gbank_watchlists.csv
      file_format = GBANK_DB.RAW_SCHEMA.cap_csv_file_format
      on_error = 'continue';                     

alter pipe transactions_pipe  refresh;
alter pipe customers_pipe  refresh;
alter pipe accounts_pipe  refresh;
alter pipe credits_pipe  refresh;
alter pipe watchlists_pipe  refresh;

-- Validating if Snowpipe ran successfully
SELECT system$pipe_status ('transactions_pipe');
SELECT system$pipe_status ('customers_pipe');
SELECT system$pipe_status ('accounts_pipe');
SELECT system$pipe_status ('credits_pipe');
SELECT system$pipe_status ('watchlists_pipe');


select * from transactions;
select * from customers;
select * from accounts;
select * from credits;
select * from watchlists;

-- Create stream for each table
CREATE OR REPLACE stream GBANK_DB.RAW_SCHEMA.transactions_stream
ON TABLE GBANK_DB.RAW_SCHEMA.transactions COMMENT = 'STREAM_CDC_1';

CREATE OR REPLACE stream GBANK_DB.RAW_SCHEMA.customers_stream
ON TABLE customers COMMENT = 'STREAM_CDC_1';

CREATE OR REPLACE stream GBANK_DB.RAW_SCHEMA.accounts_stream
ON TABLE accounts COMMENT = 'STREAM_CDC_1';;

CREATE OR REPLACE stream GBANK_DB.RAW_SCHEMA.Watchlists_stream
ON TABLE credits COMMENT = 'STREAM_CDC_1';;

SELECT GET_DDL ('STREAM', 'transactions_stream');

DESC STREAM transactions_stream;

SELECT * FROM transactions_stream;

show streams GBANK_DB.RAW_SCHEMA.transactions_stream;

desc stream transactions_stream;
-- Giving Access to query between DBT and Snowflake 
REVOKE APPLYBUDGET ON DATABASE GBANK_DB FROM ROLE PC_DBT_ROLE; 
GRANT ALL PRIVILEGES ON DATABASE GBANK_DB TO ROLE PC_DBT_ROLE; 
GRANT ALL PRIVILEGES ON SCHEMA RAW_SCHEMA TO ROLE PC_DBT_ROLE; 
GRANT SELECT ON ALL TABLES IN SCHEMA RAW_SCHEMA TO ROLE PC_DBT_ROLE;
GRANT SELECT ON FUTURE TABLES IN DATABASE GBANK_DB TO ROLE PC_DBT_ROLE;


CREATE OR REPLACE PROCEDURE GBANK_DB.STAGING_SCHEMA.PROC_UPD_ACC()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    var sql_CDC_acc_s1 = `
        INSERT INTO GBANK_DB.STAGING_SCHEMA.stg_accounts (
ACCOUNT_ID
CUSTOMER_ID
ACCOUNT_TYPE
ACCOUNT_STATUS
OPEN_DATE
CURRENT_BALANCE
CURRENCY
CREDIT_LIMIT
        )
        SELECT rt.* FROM GBANK_DB.RAW_SCHEMA.accounts ac
        LEFT JOIN GBANK_DB.RAW_SCHEMA.accounts_stream s1 
        ON ac.CUSTOMER_ID = s1.CUSTOMER_ID
    `;
    
    snowflake.execute({sqlText: sql_CDC_cer_s1});

    return ''SUCCESS'';
';

CREATE OR REPLACE PROCEDURE GBANK_DB.STAGING_SCHEMA.PROC_UPD_CRE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    var sql_CDC_cer_s1 = `
        INSERT INTO GBANK_DB.STAGING_SCHEMA.stg_credits (
CUSTOMER_ID
CREDIT_SCORE
NUMBER_OF_CREDIT_ACCOUNTS
TOTAL_CREDIT_LIMIT
TOTAL_CREDIT_USED
NUMBER_OF_LATE_PAYMENTS
BANKRUPTCIES
        )
        SELECT rt.* FROM GBANK_DB.RAW_SCHEMA.cerdits cr
        LEFT JOIN GBANK_DB.RAW_SCHEMA.credits_stream s1 
        ON cr.CUSTOMER_ID = s1.CUSTOMER_ID
    `;
    
    snowflake.execute({sqlText: sql_CDC_cer_s1});

    return ''SUCCESS'';
';

CREATE OR REPLACE PROCEDURE GBANK_DB.STAGING_SCHEMA.PROC_UPD_CUS()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    var sql_CDC_cus_s1 = `
            INSERT INTO GBANK_DB.STAGING_SCHEMA.stg_customers (
CUSTOMER_ID
FIRST_NAME
LAST_NAME
DATE_OF_BIRTH
GENDER
EMAIL
PHONE_NUMBER
ADDRESS
CITY
COUNTRY
OCCUPATION
INCOME_BRACKET
CUSTOMER_SINCE
        )
        SELECT rt.* FROM GBANK_DB.RAW_SCHEMA.customers cu 
        LEFT JOIN GBANK_DB.RAW_SCHEMA.customers_stream s1 
        ON cu.CUSTOMER_ID = s1.CUSTOMER_ID
    `;
    
    snowflake.execute({sqlText: sql_CDC_cus_s1});

    return ''SUCCESS'';
';

CREATE OR REPLACE PROCEDURE GBANK_DB.STAGING_SCHEMA.PROC_UPD_TRA()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    var sql_CDC_s1 = `
        INSERT INTO GBANK_DB.STAGING_SCHEMA.stg_transactions (
            TRANSACTION_ID,
            CUSTOMER_ID,
            TRANSACTION_DATE,
            AMOUNT,
            CURRENCY,
            TRANSACTION_TYPE,
            CHANNEL,
            MERCHANT_NAME,
            MERCHANT_CATEGORY,
            LOCATION_COUNTRY,
            LOCATION_CITY,
            IS_FLAGGED
        )
        SELECT rt.* FROM GBANK_DB.RAW_SCHEMA.transactions rt 
        LEFT JOIN GBANK_DB.RAW_SCHEMA.transactions_stream s1 
        ON s1.CUSTOMER_ID = rt.CUSTOMER_ID
    `;
    
    snowflake.execute({sqlText: sql_CDC_s1});

    return ''SUCCESS'';
';

CREATE OR REPLACE PROCEDURE GBANK_DB.STAGING_SCHEMA.PROC_UPD_WAT()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS '
    var sql_CDC_wat_s1 = `
        INSERT INTO GBANK_DB.STAGING_SCHEMA.stg_watchlists (
ENTITY_ID
ENTITY_NAME
ENTITY_TYPE
RISK_CATEGORY
LISTED_DATE
SOURCE
        )
        SELECT rt.* FROM GBANK_DB.RAW_SCHEMA.watchlists wa
        LEFT JOIN GBANK_DB.RAW_SCHEMA.watchlists_stream s1 
        ON wa.CUSTOMER_ID = s1.CUSTOMER_ID
    `;
    
    snowflake.execute({sqlText: sql_CDC_wat_s1});

    return ''SUCCESS'';
';


-- Create Functions to train model for  fraud detection
CREATE OR REPLACE FUNCTION GBANK_DB.STAGING_SCHEMA.trans_func(amount number)
RETURNS STRING
LANGUAGE SQL 
AS
$$
SELECT CASE WHEN AMOUNT > 1000 THEN 'HIGH'
        WHEN AMOUNT BETWEEN 500 AND 1000 THEN 'MEDIUM'
        ELSE 'LOW' 
		END
$$
;   

-- Giving grant access to DBT Role for querying 
GRANT USAGE on FUNCTION GBANK_DB.STAGING_SCHEMA.transac_func(NUMBER) to role PC_DBT_ROLE;
  
SELECT *, GBANK_DB.STAGING_SCHEMA.transac_func(amount) as risk_level from GBANK_DB.STAGING_SCHEMA.stg_transactions;

--function for cleaning DOB(null values and >100 and >0)
CREATE OR REPLACE FUNCTION GBANK_DB.STAGING_SCHEMA.clean_dob(customer_since DATE, date_of_birth DATE)
RETURNS DATE
LANGUAGE SQL
AS
$$
  CASE 
    -- If date_of_birth is NULL, replace it with customer_since - 18 years
    WHEN date_of_birth IS NULL THEN DATEADD(YEAR, -18, customer_since)
    -- If age is greater than 100 years or less than 0, replace with customer_since - 18 years
    WHEN DATEDIFF(YEAR, date_of_birth, CURRENT_DATE) > 100 
         OR DATEDIFF(YEAR, date_of_birth, CURRENT_DATE) < 0 THEN DATEADD(YEAR, -18, customer_since)
    -- Otherwise, keep the original date_of_birth
    ELSE date_of_birth
  END
$$;
 
 
SELECT 
    customer_id,
    first_name,
    last_name,
    clean_dob(customer_since, date_of_birth) AS cleansed_dob
FROM 
    GBANK_DB.STAGING_SCHEMA.stg_customers;
 
 
grant USAGE on FUNCTION GBANK_DB.STAGING_SCHEMA.clean_dob(DATE, DATE) to role PC_DBT_ROLE;
 
 
--UDF for cleanse first_name and last_name 
CREATE OR REPLACE FUNCTION GBANK_DB.STAGING_SCHEMA.transform_first_name(first_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
    REGEXP_REPLACE(first_name, '[^a-zA-Z0-9]', '')
$$;
 
 
CREATE OR REPLACE FUNCTION GBANK_DB.STAGING_SCHEMA.transform_last_name(last_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  REPLACE(last_name, ' ', '')
$$;
 
SELECT
  customer_id,
  transform_first_name(first_name) AS first_name_cleaned,
  transform_first_name(last_name) AS last_name_cleaned
FROM
  GBANK_DB.STAGING_SCHEMA.stg_customers;
 
GRANT USAGE ON FUNCTION GBANK_DB.STAGING_SCHEMA.transform_first_name(STRING) TO ROLE PC_DBT_ROLE;

GRANT USAGE ON FUNCTION GBANK_DB.STAGING_SCHEMA.transform_last_name(STRING) TO ROLE PC_DBT_ROLE;
 
 
CREATE OR REPLACE PROCEDURE GBANK_DB.STAGING_SCHEMA.merge_cleansed_customer_data()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Create a temporary table with cleansed data using UDFs
    CREATE OR REPLACE TEMPORARY TABLE     GBANK_DB.STAGING_SCHEMA.customers_temp AS
    SELECT
        customer_id,
        transform_first_name(first_name) AS first_name,
        transform_last_name(last_name) AS last_name,
        cleanse_dob(customer_since, date_of_birth) AS date_of_birth,
        gender,
        email,
        phone_number,
        address,
        city,
        country,
        occupation,
        income_bracket,
        customer_since
    FROM
            GBANK_DB.STAGING_SCHEMA.stg_customers;
 
    -- Merge the cleansed data into the original customer table
    MERGE INTO     GBANK_DB.STAGING_SCHEMA.stg_customers AS target
    USING customer_temp AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            target.first_name = source.first_name,
            target.last_name = source.last_name,
            target.date_of_birth = source.date_of_birth,
            target.gender = source.gender,
            target.email = source.email,
            target.phone_number = source.phone_number,
            target.address = source.address,
            target.city = source.city,
            target.country = source.country,
            target.occupation = source.occupation,
            target.income_bracket = source.income_bracket,
            target.customer_since = source.customer_since
    WHEN NOT MATCHED THEN
        INSERT (customer_id, first_name, last_name, date_of_birth, gender, email, phone_number, address, city, country, occupation, income_bracket, customer_since)
        VALUES (source.customer_id, source.first_name, source.last_name, source.date_of_birth, source.gender, source.email, source.phone_number, source.address, source.city, source.country, source.occupation, source.income_bracket, source.customer_since);
 
    RETURN 'Merge operation completed successfully.';
END;
$$;
 
CALL     GBANK_DB.STAGING_SCHEMA.merge_cleansed_customer_data();
 
 
select * from stg_customers;
-----------------------------------

-- Create table on staging_schema transactions data
CREATE OR REPLACE TABLE GBank_DB.staging_schema.stg_transactions(
    transaction_id STRING,    
    customer_id STRING,    
    transaction_date TIMESTAMP_NTZ,    
    amount FLOAT,
    currency STRING,    
    transaction_type STRING,    
    channel STRING,    
    merchant_name STRING,    
    merchant_category STRING,    
    location_country STRING,    
    location_city STRING,    
    is_flagged BOOLEAN
)CLUSTER BY (customer_id, DATE(transaction_date));


-- Create table on staging_schema customer data
CREATE OR REPLACE TABLE GBANK_DB.staging_schema.stg_customers (
    customer_id STRING,    
    first_name STRING,    
    last_name STRING,    
    date_of_birth DATE,
    gender STRING,    
    email STRING,    
    phone_number STRING,    
    address STRING,    
    city STRING,    
    country STRING,
    occupation STRING,
    income_bracket STRING,
    customer_since DATE
)CLUSTER BY (income_bracket, DATE(customer_since));


-- Create table on staging_schema account data
CREATE OR REPLACE TABLE GBANK_DB.staging_schema.stg_accounts (
    account_id STRING,
    customer_id STRING,
    account_type STRING,
    account_status STRING,
    open_date DATE,
    current_balance FLOAT,
    currency STRING,
    credit_limit FLOAT
) CLUSTER BY (account_status, credit_limit);

SELECT * from PC_DBT_DB.DBT_NNPVIMALRAJCH.TRANSACTION;
-- Create table on staging_schema credit bureau data
CREATE OR REPLACE TABLE GBANK_DB.staging_schema.stg_credits (
    customer_id STRING,
    credit_score INT,
    number_of_credit_accounts INT,
    total_credit_limit FLOAT,
    total_credit_used FLOAT,
    number_of_late_payments INT,
    bankruptcies INT
)CLUSTER BY (credit_score , total_credit_used );

-- Create table on staging_schema watchlist data
CREATE OR REPLACE TABLE GBANK_DB.staging_schema.stg_watchlists (
    entity_id STRING,
    entity_name STRING,
    entity_type STRING,
    risk_category STRING,
    listed_date DATE,
    source STRING
)CLUSTER BY (risk_category ,source);

--  Create a TASK which run every 1 min and look for data in Stream PERSON_NESTED_STREAM, 
--if data found in Stream then task will EXECUTE if not TASK will be SKIPPED without any doing anything.
CREATE OR REPLACE TASK GBANK_DB.STAGING_SCHEMA.STG_TRANSACTIONS_TASK
WAREHOUSE='compute_wh' 
SCHEDULE='1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('GBANK_DB.RAW_SCHEMA.transactions_stream')
AS
CALL GBANK_DB.STAGING_SCHEMA.proc_upd_tra();


CREATE OR REPLACE PROCEDURE GBANK_DB.STAGING_SCHEMA.proc_upd_tra()
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS OWNER
AS
$$
    var sql_CDC_s1 = `
        INSERT INTO GBANK_DB.STAGING_SCHEMA.stg_transactions (
            TRANSACTION_ID,
            CUSTOMER_ID,
            TRANSACTION_DATE,
            AMOUNT,
            CURRENCY,
            TRANSACTION_TYPE,
            CHANNEL,
            MERCHANT_NAME,
            MERCHANT_CATEGORY,
            LOCATION_COUNTRY,
            LOCATION_CITY,
            IS_FLAGGED
        )
        SELECT rt.* FROM GBANK_DB.RAW_SCHEMA.transactions rt 
        LEFT JOIN GBANK_DB.RAW_SCHEMA.transactions_stream s1 
        ON s1.CUSTOMER_ID = rt.CUSTOMER_ID
    `;
    
    snowflake.execute({sqlText: sql_CDC_s1});

    return 'SUCCESS';
$$;


call GBANK_DB.STAGING_SCHEMA.proc_upd_tra();

		
ALTER TASK GBANK_DB.STAGING_SCHEMA.STG_TRANSACTIONS_TASK RESUME;

DESC TASK GBANK_DB.STAGING_SCHEMA.STG_TRANSACTIONS_TASK;


TRUNCATE TABLE GBANK_DB.STAGING_SCHEMA.stg_transactions;
TRUNCATE TABLE GBANK_DB.STAGING_SCHEMA.stg_transactions;
TRUNCATE TABLE GBANK_DB.STAGING_SCHEMA.stg_transactions;
TRUNCATE TABLE GBANK_DB.STAGING_SCHEMA.stg_transactions;
TRUNCATE TABLE GBANK_DB.STAGING_SCHEMA.stg_transactions;


SELECT * FROM table(INFORMATION_SCHEMA.TAST_HISTORY()) ORDER BY schedule_time;

select * from GBANK_DB.STAGING_SCHEMA.stg_transactions;
select * from GBANK_DB.STAGING_SCHEMA.stg_customers;
select * from GBANK_DB.STAGING_SCHEMA.stg_accounts;
select * from GBANK_DB.STAGING_SCHEMA.stg_credits;
select * from GBANK_DB.STAGING_SCHEMA.stg_watchlists;

-- Create the transactions table with a cluster key for Query Pruning

CREATE OR REPLACE ROLE risk_management;
-- Masking policy for sensitive customer data
CREATE MASKING POLICY mask_credit_data AS
  (val INT) RETURNS INT ->
  CASE
    WHEN current_role() IN ('risk_management') THEN NULL
    ELSE val
  END;
-- Apply masking policy to credit balance
ALTER TABLE GBANK_DB.security_schema.credits_t
  MODIFY COLUMN credit_score SET MASKING POLICY mask_credit_data;
ALTER TABLE GBANK_DB.security_schema.credits_t
  MODIFY COLUMN total_credit_limit SET MASKING POLICY mask_credit_data;


CREATE OR REPLACE ROLE customer_service;
CREATE OR REPLACE ROLE marketing;


-- Masking policy for sensitive account data
CREATE MASKING POLICY GBANK_DB.security_schema.mask_sensitive_account_data AS
  (val FLOAT) RETURNS FLOAT ->
  CASE
    WHEN current_role() IN ('customer_service', 'marketing') THEN NULL
    ELSE val
  END;

-- Apply masking policy to account current balance
ALTER TABLE GBANK_DB.security_schema.accounts_t
  MODIFY COLUMN current_balance SET MASKING POLICY mask_sensitive_account_data;
  
  
CREATE OR REPLACE ROLE sales_team;
-- Implement Column-Level Encryption for sensitive account data
CREATE MASKING POLICY GBANK_DB.security_schema.mask_customer_info AS
  (val STRING) RETURNS STRING ->
  CASE
    WHEN current_role() IN ('sales_team') THEN 'MASKED'
    ELSE val
  END;
  
-- Apply masking policy to customer info
ALTER TABLE GBANK_DB.security_schema.customers_t
  MODIFY COLUMN first_name SET MASKING POLICY mask_customer_info;
ALTER TABLE GBANK_DB.security_schema.customers_t
  MODIFY COLUMN last_name SET MASKING POLICY mask_customer_info;  
ALTER TABLE GBANK_DB.security_schema.customers_t
  MODIFY COLUMN email SET MASKING POLICY mask_customer_info;
ALTER TABLE GBANK_DB.security_schema.customers_t
  MODIFY COLUMN phone_number SET MASKING POLICY mask_customer_info;
  ALTER TABLE GBANK_DB.security_schema.transactions_t
  MODIFY COLUMN amount SET MASKING POLICY mask_sensitive_transaction_data;


-- Create a Role Mapping Table and inserting values
CREATE OR REPLACE TABLE GBank_DB.SECURITY_SCHEMA.role_mapping (
    role_name STRING,
    customer_id STRING
);
INSERT INTO GBank_DB.SECURITY_SCHEMA.role_mapping (role_name, customer_id) VALUES
('sales_team', 'CUST001'),
('sales_team', 'CUST002'),
('marketing_team', 'CUST003');


-- Create a row access policy to filter rows based on the role.
CREATE OR REPLACE ROW ACCESS POLICY customer_row_access_policy
AS (customer_id STRING) RETURNS BOOLEAN ->
EXISTS (
    SELECT *
    FROM GBank_DB.SECURITY_SCHEMA.role_mapping
    WHERE role_mapping.customer_id = customer_id
    AND role_mapping.role_name = CURRENT_ROLE()
);

ALTER TABLE GBank_DB.security_schema.customers_t
ADD ROW ACCESS POLICY customer_row_access_policy ON (customer_id);


