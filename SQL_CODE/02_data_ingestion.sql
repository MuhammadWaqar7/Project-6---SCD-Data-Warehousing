create or replace stage customer_ext_stage
  url='s3://nifiprojectwaqar/IEAnuZ.csv'
  credentials=(aws_key_id='' aws_secret_key='')
  file_format =csv_format;
  
show stages;
list @customer_ext_stage;


create or replace pipe customer_s3_pipe
  auto_ingest = true
  as
  copy into customer_raw
  from @customer_ext_stage/customer_20210806183233.csv
  file_format = csv_format
  ;
  
show pipes;
select SYSTEM$PIPE_STATUS('customer_s3_pipe');

select count(*) from customer_raw;

CREATE OR REPLACE PIPE customer_s3_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO customer_raw
  FROM @customer_ext_stage
  FILE_FORMAT = (FORMAT_NAME = csv_format)
  PATTERN = '.customer_.\\.csv';




  --- deep
  -- First, drop the existing pipe if it exists
DROP PIPE IF EXISTS customer_s3_pipe;

-- Create the stage (this looks correct)
CREATE OR REPLACE STAGE customer_ext_stage
  URL='s3://nifiprojectwaqar/IEAnuZ.csv'
  CREDENTIALS=(aws_key_id='' aws_secret_key='')
  FILE_FORMAT = csv_format;

-- Create the pipe with correct PATTERN
CREATE OR REPLACE PIPE customer_s3_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO customer_raw
  FROM @customer_ext_stage
  FILE_FORMAT = (FORMAT_NAME = csv_format)
  PATTERN = '.*customer_.*\\.csv';

LIST @customer_ext_stage;
  COPY INTO customer_raw
FROM @customer_ext_stage
FILE_FORMAT = (FORMAT_NAME = csv_format)
PATTERN = '.*customer_.*\\.csv';

CREATE OR REPLACE PIPE customer_s3_pipe
  AUTO_INGEST = TRUE
  AS
COPY INTO customer_raw
FROM @customer_ext_stage
FILE_FORMAT = (FORMAT_NAME = csv_format)
PATTERN = '.*IEAnuZ.*\\.csv';
