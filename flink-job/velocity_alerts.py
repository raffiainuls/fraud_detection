import logging 
import sys 
import traceback
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath 
from helper.function import create_env, create_table_if_not_exists
from transactions.lib import create_transactions
from velocity_alerts.lib import create_velocity_alerts, insert_into_velocity_alerts

table_env = create_env()

trasactions = create_transactions()
create_table_if_not_exists(table_env, "transactions", trasactions)

velocity_alerts = create_velocity_alerts()
create_table_if_not_exists(table_env, "velocity_alerts", velocity_alerts)

insert_into_velocity_alerts(table_env)