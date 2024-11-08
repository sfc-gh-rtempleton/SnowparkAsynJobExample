
create or replace stage STAGE_DASHBOARD
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = (ENABLE = TRUE);


/**
    Define STREAMLIT object
**/

CREATE OR REPLACE STREAMLIT Snowflake_Usage_Dashboard 
 ROOT_LOCATION = @STAGE_DASHBOARD
 MAIN_FILE = 'account_overview.py'
 QUERY_WAREHOUSE = SNOWFLAKE_WH;


 put file://environment.yml @STAGE_DASHBOARD auto_compress = False overwrite = True;

-- root streamit files
  put file://*.py @STAGE_DASHBOARD auto_compress = False overwrite = True;
  

-- pages files
 put file://pages/*.py @STAGE_DASHBOARD/pages auto_compress = False overwrite = True;