CREATE DATABASE IF NOT EXISTS GBANK_DB;
CREATE SCHEMA IF NOT EXISTS GBANK_DB.raw_schema;
CREATE SCHEMA IF NOT EXISTS GBANK_DB.transform_schema;
CREATE SCHEMA IF NOT EXISTS GBANK_DB.analytics_schema;
CREATE SCHEMA IF NOT EXISTS GBANK_DB.security_schema;

-- Create table for raw transactions data
CREATE OR REPLACE TABLE GBank_DB.RAW_SCHEMA.transactions_t(
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

-- Create table for raw customer data
CREATE OR REPLACE TABLE GBANK_DB.RAW_SCHEMA.customers_t (
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

-- Create table for raw account data
CREATE OR REPLACE TABLE GBANK_DB.RAW_SCHEMA.accounts_t (
    account_id STRING,
    customer_id STRING,
    account_type STRING,
    account_status STRING,
    open_date DATE,
    current_balance FLOAT,
    currency STRING,
    credit_limit FLOAT;
);
-- Create table for raw credit bureau data
CREATE OR REPLACE TABLE GBANK_DB.RAW_SCHEMA.credits_t (
    customer_id STRING,
    credit_score INT,
    number_of_credit_accounts INT,
    total_credit_limit FLOAT,
    total_credit_used FLOAT,
    number_of_late_payments INT,
    bankruptcies INT
);
-- Create table for raw watchlist data
CREATE OR REPLACE TABLE GBANK_DB.RAW_SCHEMA.watchlists_t (
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
storage_allowed_locations = ('s3://aws-gbank-csv/gbank-raw-data-csv/');

-- Describe integration object to fetch external_id and to be used in s3 Trust Partnership
desc integration S3_INTEG_RAW_CSV;
  
-- Create csv file format
create or replace file format GBANK_DB.RAW_SCHEMA.cap_csv_file_format
type = 'csv';

-- Create external stage object
create or replace stage GBANK_DB.RAW_SCHEMA.sta_gbank_s3_data
URL = 's3://aws-gbank-csv/gbank-raw-data-csv/transactions.csv'
STORAGE_INTEGRATION = S3_INTEG_RAW_CSV
file_format = GBANK_DB.RAW_SCHEMA.cap_csv_file_format;


list @sta_gbank_s3_data;


--Create pipe for transaction raw data
create or replace pipe transactions_t_pipe 
auto_ingest = true as
copy into transactions_t 
from @GBANK_DB.RAW_SCHEMA.sta_gbank_s3_data/transactions.csv
      file_format = GBANK_DB.RAW_SCHEMA.cap_csv_file_format
      on_error = 'continue';

 --Create pipe for transaction raw data
create or replace pipe customers_t_pipe 
auto_ingest = true as
copy into transactions_t 
from @GBANK_DB.RAW_SCHEMA.sta_gbank_s3_data/customers.csv
      file_format = GBANK_DB.RAW_SCHEMA.cap_csv_file_format
      on_error = 'continue';

--Create pipe for transaction raw data
create or replace pipe accounts_t_pipe 
auto_ingest = true as
copy into transactions_t 
from @GBANK_DB.RAW_SCHEMA.sta_gbank_s3_data/accounts.csv
      file_format = GBANK_DB.RAW_SCHEMA.cap_csv_file_format
      on_error = 'continue';


--Create pipe for transaction raw data
create or replace pipe credits_t_pipe 
auto_ingest = true as
copy into transactions_t 
from @GBANK_DB.RAW_SCHEMA.sta_gbank_s3_data/credits.csv
      file_format = GBANK_DB.RAW_SCHEMA.cap_csv_file_format
      on_error = 'continue';


--Create pipe for transaction raw data
create or replace pipe watchlists_t_pipe 
auto_ingest = true as
copy into transactions_t 
from @GBANK_DB.RAW_SCHEMA.sta_gbank_s3_data/watchlists.csv
      file_format = GBANK_DB.RAW_SCHEMA.cap_csv_file_format
      on_error = 'continue';                     

SHOW PIPES;

alter pipe transactions_t_pipe  refresh;
alter pipe customers_t_pipe  refresh;
alter pipe accounts_t_pipe  refresh;
alter pipe credits_t_pipe  refresh;
alter pipe watchlists_t_pipe  refresh;

-- Validating if Snowpipe ran successfully
SELECT system$pipe_status ('transactions_t_pipe');
SELECT system$pipe_status ('customers_t_pipe');
SELECT system$pipe_status ('accounts_t_pipe');
SELECT system$pipe_status ('credits_t_pipe');
SELECT system$pipe_status ('watchlists_t_pipe');


select * from transactions_t;
select * from customers_t;
select * from accounts_t;
select * from credits_t;
select * from watchlists_t;

-- Giving Access to query between DBT and Snowflake 
REVOKE APPLYBUDGET ON DATABASE GBANK_DB FROM ROLE PC_DBT_ROLE; 
GRANT ALL PRIVILEGES ON DATABASE GBANK_DB TO ROLE PC_DBT_ROLE; 
GRANT ALL PRIVILEGES ON SCHEMA RAW_SCHEMA TO ROLE PC_DBT_ROLE; 
GRANT SELECT ON ALL TABLES IN SCHEMA RAW_SCHEMA TO ROLE PC_DBT_ROLE;
GRANT SELECT ON FUTURE TABLES IN DATABASE GBANK_DB TO ROLE PC_DBT_ROLE;

-- Create Functions to train model for  fraud detection
CREATE OR REPLACE FUNCTION trans_func(amount number)
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
GRANT USAGE on FUNCTION transac_func(NUMBER) to role PC_DBT_ROLE;
  
SELECT *, transac_func(amount) as risk_level from transactions_t;

-- Create the transactions table with a cluster key for Query Pruning
CREATE OR REPLACE TABLE GBank_DB.RAW_SCHEMA.transactions_t(
    transaction_id STRING,    
    customer_id STRING,    
    transaction_date TIMESTAMP_NTZ,    
    amount FLOAT,
    currency STRING,    
    transaction_type STRING,    
    merchant_name STRING,    
)
CLUSTER BY (customer_id, DATE(transaction_date));


-- Create the customers_accounts table with a cluster key
CREATE OR REPLACE TABLE GBank_DB.RAW_SCHEMA.customers_accounts_t
CLUSTER BY (customer_id, account_type) AS
SELECT 
    c.customer_id STRING,
    c.first_name STRING,
    c.last_name STRING,
    c.date_of_birth,
    c.gender STRING,
    c.email STRING,
    c.phone_number STRING,
    c.income_bracket STRING,
    c.customer_since DATE,
    a.account_id STRING,
    a.currency STRING,
    a.credit_limit STRING
FROM 
    GBANK_DB.RAW_SCHEMA.customers_t c JOIN 
    GBANK_DB.RAW_SCHEMA.accounts_t a
ON c.customer_id = a.customer_id;

-- 3. Create Secure Views for Data Sharing
CREATE OR REPLACE SECURE VIEW GBank_DB.ANALYTICS_SCHEMA.sv_customers AS
SELECT 
    customer_id,
    first_name,
    last_name,
    date_of_birth,
    gender,
    email,
    phone_number,
    address,
    city,
    country,
    occupation,
    income_bracket,
    customer_since
FROM GBank_DB.RAW_SCHEMA.customers_t;


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


