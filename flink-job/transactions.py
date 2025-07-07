import logging 
import sys 
import traceback
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import ObjectPath 
from helper.function import create_env, create_table_if_not_exists
from transactions.lib import create_transactions
from big_amount.lib import create_big_amount, insert_into_big_amount


table_env = create_env()

transactions = create_transactions()
create_table_if_not_exists(table_env, "transactions", transactions)

big_amount = create_big_amount()
create_table_if_not_exists(table_env, "big_amount", big_amount)

insert_into_big_amount(table_env)
