import snowflake.connector
import pandas as pd
from sqlalchemy import create_engine
import pyhive
from pyhive import hive
import os

# Snowflake Connection Details
sf_account = 'pranali_sf_account'
sf_user = 'pranali'
sf_password = 'psawant23'
sf_database = 'employee_DB'
sf_schema = 'p_schema'

# Hive Connection Details
hive_host = 'myfirst_hive_host'
hive_port = 10000

# Connect to Hive
def get_hive_data(query):
    conn = hive.connect(host=hive_host, port=hive_port)
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    return pd.DataFrame(result, columns=columns)

# Extract data from Hive
hive_query = 'SELECT * FROM employee_performance;'  # Adjust your Hive query
hive_data = get_hive_data(hive_query)

# Process data (example: calculating skill improvement scores)
hive_data['skill_improvement'] = hive_data['current_skill'] - hive_data['initial_skill']

# Connect to Snowflake
def connect_snowflake():
    conn = snowflake.connector.connect(
        user=sf_user,
        password=sf_password,
        account=sf_account,
        warehouse='my_warehouse',
        database=sf_database,
        schema=sf_schema
    )
    return conn

# Upload data to Snowflake
def upload_to_snowflake(dataframe, table_name):
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    # Create Snowflake table if it doesn't exist (basic example)
    cursor.execute(f"CREATE OR REPLACE TABLE {table_name} (employee_id INT, name STRING, current_skill INT, initial_skill INT, skill_improvement INT)")
    
    # Convert DataFrame to CSV and upload to Snowflake
    temp_file = '/tmp/temp_data.csv'
    dataframe.to_csv(temp_file, index=False)
    
    # Upload the file to Snowflake (assuming Snowflake stage setup is done)
    cursor.execute(f"PUT file://{temp_file} @your_stage_name")
    cursor.execute(f"COPY INTO {table_name} FROM @your_stage_name FILE_FORMAT = (TYPE = 'CSV')")

    # Clean up
    os.remove(temp_file)
    conn.close()

# Call the function to upload data
upload_to_snowflake(hive_data, 'employee_performance')
