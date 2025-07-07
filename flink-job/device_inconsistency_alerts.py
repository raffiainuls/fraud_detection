import logging 
import sys 
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath 
from helper.function import create_env, create_table_if_not_exists
from transactions.lib import create_transactions
from device_inconsistency_alerts.lib import create_device_alerts, insert_into_device_alerts

table_env = create_env()

transactions = create_transactions()
create_table_if_not_exists(table_env, "transactions", transactions)

device_alerts = create_device_alerts()
create_table_if_not_exists(table_env, "device_alerts", device_alerts)

insert_into_device_alerts(table_env)