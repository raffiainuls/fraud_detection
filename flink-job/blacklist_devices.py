import logging 
import sys 
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath 
from helper.function import create_env, create_table_if_not_exists
from transactions.lib import create_transactions
from blacklist_customers_devices.lib import create_blacklist_devices, insert_into_devices_blacklist, create_blacklist


table_env = create_env()

transactions = create_transactions()
create_table_if_not_exists(table_env, "transactions", transactions)


blacklist = create_blacklist()
create_table_if_not_exists(table_env, "blacklist", blacklist)

blacklist_devices = create_blacklist_devices()
create_table_if_not_exists(table_env, "blacklist_devices", blacklist_devices)

insert_into_devices_blacklist(table_env)