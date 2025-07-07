import logging 
import sys 
import traceback
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath 
from helper.function import create_env, create_table_if_not_exists
from transactions.lib import create_transactions
from geo_locations_alerts.lib import create_geolocation_alerts, insert_into_geolocation_alerts


table_env = create_env()

transactions = create_transactions()
create_table_if_not_exists(table_env, "transactions", transactions)

geolocation_alerts = create_geolocation_alerts()
create_table_if_not_exists(table_env, "geolocation_alerts", geolocation_alerts)

insert_into_geolocation_alerts(table_env)