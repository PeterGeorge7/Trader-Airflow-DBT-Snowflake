import snowflake.connector

def connect_to_snowflake():
    # Load Snowflake credentials from environment variables
    import os
    from dotenv import load_dotenv

    load_dotenv()

    snowflake_url = os.getenv('snowflake_url')
    snowflake_user = os.getenv('snowflake_user')
    snowflake_password = os.getenv('snowflake_password')
    snowflake_account = os.getenv('snowflake_account')
    snowflake_warehouse = os.getenv('snowflake_warehouse')
    snowflake_database = os.getenv('snowflake_database')
    snowflake_schema = os.getenv('snowflake_schema')

    # Connect to Snowflake
    ctx = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
    )
    
    return ctx


# put file csv into snowflake stage
def upload_csv_to_snowflake(ctx,file_path, stage_name):
    cs = ctx.cursor()
    try:
        cs.execute(f"PUT file://{file_path} @{stage_name}")
        print(f"File {file_path} uploaded to stage {stage_name}")
        load_table_from_stage(cs, stage_name, "world_bank_raw", file_path.split('/')[-1])
    except Exception as e:
        print(f"Failed to upload file: {e}")
    finally:
        cs.close()


def load_table_from_stage(cs, stage_name, table_name, file_name):
    try:
        cs.execute(f"COPY INTO {table_name} FROM @{stage_name}/{file_name}")
        print(f"Data loaded from stage {stage_name} into table {table_name}")
    except Exception as e:
        print(f"Failed to load data into table: {e}")

if __name__ == "__main__":
    ctx = connect_to_snowflake()
    upload_csv_to_snowflake(ctx, 'world_bank_data.csv', 'MY_INT_STAGE')