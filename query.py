import time
import datetime
from loguru import logger
API_KEY = "8McV5QVxTukQuuDEE0iLetT1SinaGwS6"
HEADER = {"x-dune-api-key" : API_KEY}
BASE_URL = "<your-api"
import json
from requests import get, post
import pandas as pd
import re
import sqlite3

logger.add("my_log.log", rotation="10 MB", backtrace=True, diagnose=True)  # Сохранение логов в файл с ротацией по размеру

def make_api_url(module, action, ID):
    """
    We shall use this function to generate a URL to call the API.
    """

    url = BASE_URL + module + "/" + ID + "/" + action

    return url

def execute_query_with_params(query_id, param_dict):
    """
    Takes in the query ID. And a dictionary containing parameter values.
    Calls the API to execute the query.
    Returns the execution ID of the instance which is executing the query.
    """

    url = make_api_url("query", "execute", query_id)

    response = post(url, headers=HEADER, json={"query_parameters" : param_dict})
    execution_id = response.json()['execution_id']

    return execution_id

def execute_query(query_id, engine="medium"):
    """
    Takes in the query ID and engine size.
    Specifying the engine size will change how quickly your query runs.
    The default is "medium" which spends 10 credits, while "large" spends 20 credits.
    Calls the API to execute the query.
    Returns the execution ID of the instance which is executing the query.
    """

    url = make_api_url("query", "execute", query_id)
    params = {
        "performance": engine,
    }
    response = post(url, headers=HEADER, params=params)
    execution_id = response.json()['execution_id']
    return execution_id

def get_query_status(execution_id):
    """
    Takes in an execution ID.
    Fetches the status of query execution using the API
    Returns the status response object
    """

    url = make_api_url("execution", "status", execution_id)
    response = get(url, headers=HEADER)

    return response

def get_query_results(execution_id):
    """
    Takes in an execution ID.
    Fetches the results returned from the query using the API
    Returns the results response object
    """

    url = make_api_url("execution", "results", execution_id)
    response = get(url, headers=HEADER)

    return response

def cancel_query_execution(execution_id):
    """
    Takes in an execution ID.
    Cancels the ongoing execution of the query.
    Returns the response object.
    """

    url = make_api_url("execution", "cancel", execution_id)
    response = get(url, headers=HEADER)

    return response

def get_stats(wallet):
    parameters = {"wallet_address": f'{wallet} '}
    execute_id = execute_query_with_params("2928920", parameters)
    x = 0
    while x != 1:
        try:
            status = get_query_status(execute_id).json()['state']
            if status == 'QUERY_STATE_COMPLETED':
                x = 1
            logger.info("Current status of query: " + status)
            time.sleep(1)
        except:
            logger.error('Connection failed')
            get_stats(wallet)
    try:
        response = get_query_results(execute_id).json()['result']
    except:
        logger.error('Connection failed')
        get_stats(wallet)
    sum = 0
    month_list = ''
    source_chain_list = []
    destination_chain_list = []
    for i in response['rows']:
        try:
            sum = sum + i['amount_usd']
        except:
            logger.error("The NAN amount")
        month_list = month_list+i['block_time']+'\n'
        source_chain_list.append(i['source_chain_name'])
        destination_chain_list.append(i['destination_chain_name'])
    dates = re.findall(r"(\d{4}-\d{2}).+", month_list)
    total_amount = sum
    unique_months = len(set(dates))
    source_chains = len(set(source_chain_list))
    destination_chains = len(set(destination_chain_list))
    logger.info(''' Checked!
    Wallet: {wallet}
    Total volume on wallet is - {total_amount},
    the number of source chains is - {source_chains},
    the number of destination chains is - {destination_chains},
    the number of unique months is - {unique_months}
    '''.format(wallet=wallet, total_amount=total_amount, source_chains=source_chains,
               destination_chains=destination_chains, unique_months=unique_months))
    return total_amount, unique_months, source_chains, destination_chains

def get_all_wallets_stats(wallets, user):
    conn = sqlite3.connect('user_stats_db.sqlite')
    cur = conn.cursor()
    table_name = 'User_'+user
    cur.executescript("""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (
    id   INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    address TEXT UNIQUE,
    total_amount INTEGER,
    unique_months INTEGER,
    source_chains INTEGER,
    destination_chains INTEGER
        )
    """.format(table_name=table_name))
    logger.info('Created table for user - ' + user)
    for i in wallets:
        x = 0
        while x == 0:
            try:
                total_amount, unique_months, source_chains, destination_chains = get_stats(i)
                x = 1
            except Exception:
                logger.error("Troubls with connection")
        cur.execute("""
        INSERT OR IGNORE INTO {table_name} (address, total_amount, unique_months, source_chains, destination_chains)
        VALUES (?, ?, ?, ?, ?)
        """.format(table_name=table_name), (i, total_amount, unique_months, source_chains, destination_chains))
    conn.commit()
    return table_name
  #wallets for tests
wallets = '0xF7cE..., 0x38c6..., 0x5c0d...'
wallets_1 = ['0xF7cE...', '0x38c6...', '0x5c0d...']
def get_stats_v2(wallets, user):
    wallets_v1 = wallets.split(", ")
    wallets_v1l = wallets.lower().split(", ")
    print(wallets_v1)
    parameters = {"wallets": f'{wallets} '}
    execute_id = execute_query_with_params("2946081", parameters)
    x = 0
    while x != 1:
        try:
            status = get_query_status(execute_id).json()['state']
            if status == 'QUERY_STATE_COMPLETED':
                x = 1
            logger.info("Current status of query: " + status)
            time.sleep(1)
        except:
            logger.error('Connection failed')
            get_stats_v2(wallets)
    try:
        response = get_query_results(execute_id).json()['result']['rows']
    except:
        logger.error('Connection failed')
        get_stats_v2(wallets)
    print(response)
    conn = sqlite3.connect('user_stats_db.sqlite')
    cur = conn.cursor()
    table_name = 'User_' + user
    cur.executescript("""
       DROP TABLE IF EXISTS {table_name};
       CREATE TABLE {table_name} (
       id   INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
       address TEXT UNIQUE,
       total_amount INTEGER,
       unique_months INTEGER,
       source_chains INTEGER,
       destination_chains INTEGER
           )
       """.format(table_name=table_name))
    logger.info('Created table for user - ' + user)
    address_list = []
    address_list_normal = []
    sum = 0
    month_list = ''
    source_chain_list = []
    destination_chain_list = []
    for i in response:
        if i['user_address'] in address_list:
            try:
                sum = sum + i['amount_usd']
            except Exception as e:
                logger.error(e)
            month_list = month_list + i['block_time'] + '\n'
            source_chain_list.append(i['source_chain_name'])
            destination_chain_list.append(i['destination_chain_name'])

        else:
            if sum != 0:
                total_amount = sum
                address = address_list_normal[-1]
                dates = re.findall(r"(\d{4}-\d{2}).+", month_list)
                unique_months = len(set(dates))
                source_chains = len(set(source_chain_list))
                destination_chains = len(set(destination_chain_list))
                cur.execute(f"""
                        INSERT INTO {table_name} (address, total_amount, unique_months, source_chains, destination_chains)
                    VALUES (?, ?, ?, ?, ?)
                        """, (address, total_amount, unique_months, source_chains, destination_chains))
                conn.commit()
                logger.info(''' Checked!
                    Wallet: {wallet}
                    Total volume on wallet is - {total_amount},
                    the number of source chains is - {source_chains},
                    the number of destination chains is - {destination_chains},
                    the number of unique months is - {unique_months}
                    '''.format(wallet=address, total_amount=total_amount, source_chains=source_chains,
                               destination_chains=destination_chains, unique_months=unique_months))
            sum = 0
            month_list = ''
            source_chain_list = []
            destination_chain_list = []
            address_list.append(i['user_address'])
            address_list_normal.append(wallets_v1[wallets_v1l.index(i['user_address'])])

            try:
                sum = sum + i['amount_usd']
            except Exception as e:
                logger.error(e)
            month_list = month_list + i['block_time'] + '\n'
            source_chain_list.append(i['source_chain_name'])
            destination_chain_list.append(i['destination_chain_name'])
    total_amount = sum
    address = address_list_normal[-1]
    dates = re.findall(r"(\d{4}-\d{2}).+", month_list)
    unique_months = len(set(dates))
    source_chains = len(set(source_chain_list))
    destination_chains = len(set(destination_chain_list))
    cur.execute(f"""
                            INSERT INTO {table_name} (address, total_amount, unique_months, source_chains, destination_chains)
                        VALUES (?, ?, ?, ?, ?)
                            """, (address, total_amount, unique_months, source_chains, destination_chains))
    conn.commit()
    logger.info(''' Checked!
        Wallet: {wallet}
        Total volume on wallet is - {total_amount},
        the number of source chains is - {source_chains},
        the number of destination chains is - {destination_chains},
        the number of unique months is - {unique_months}
        '''.format(wallet=address, total_amount=total_amount, source_chains=source_chains,
                   destination_chains=destination_chains, unique_months=unique_months))
    return table_name
