import logging 
import sys 
import traceback
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath 


# setup logging 
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def create_env():
    logger.info("Starting Pyflink environment setup........")
    env_setting = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_setting)
    logger.info("TableEnvironment created.")
    return table_env


def create_table_if_not_exists(table_env, table_name: str, create_sql:str):
    catalog = table_env.get_current_catalog()
    database = table_env.get_current_database()
    obj_path = ObjectPath(database, table_name)

    catalog_obj = table_env.get_catalog(catalog)
    if catalog_obj is not None and not catalog_obj.table_exists(obj_path):
        print(f"Creating table: {table_name}....")
        table_env.execute_sql(create_sql)
        print(f"Table {table_name} created.")
    else:
        print(f"Table {table_name} already exists or catalog not found.")