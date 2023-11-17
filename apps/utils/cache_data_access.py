import os
import sys
import time
import signal
from datetime import datetime, timedelta,timezone
import json

from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError

from common_libraries.classes.broker.manager import BrokerManager
from common_libraries.classes.database.trade import Trade
from common_libraries.classes.database.position import Position
from common_libraries.classes.database.trading_account import TradingAccount
from common_libraries.classes.database.user import User
from common_libraries.classes.database.remarks import Remarks
from common_libraries.classes.database.balance import Balance
from common_libraries.classes.database.withdrawal import Withdrawal
from common_libraries.classes.database.symbols import Symbols

import common_libraries.functions as functions
import common_libraries.utils as utils


remarks_internal_cache = list(Remarks().find({}))
remarks_last_update = datetime.now()

def verify_email(email):
    if os.getenv('TEST_ENVIRONMENT', 'False') == 'True':
        if 'cryptofundtrader.com' in email:
            return True
        else:
            return False
    else:
        if 'cryptofundtrader.com' in email:
            return False
        else:
            return True
        
        

# BROKER ACCOUNTS
def divide_list(l, n):
    """
    Divide la lista l en n trozos iguales o casi iguales.
    :param l: Lista a dividir
    :param n: Número de trozos
    :return: Una lista de listas
    """
    avg = len(l) / float(n)
    return [l[int(round(avg * i)): int(round(avg * (i + 1)))] for i in range(n)]



def broker_accounts_to_set(broker_accounts):
    '''
        Convert broker accounts to a set
    '''
    broker_accounts_data = {}
    for ta in broker_accounts:
        broker_accounts_data[ta['mt_user_id'] + '_' + ta['broker']] = ta

    return broker_accounts_data


def get_broker_accounts_chunked(tm):
    '''
        Get all broker accounts chunked,
        obtain the chunk size from the environment variable CHUNK_SIZE
        obtain the chunk number from the environment variable CHUNK_NUMBER
    '''
    datemilliseconds = datetime.now()

    all_accounts =  tm.broker_manager.get_all_accounts(1,  0)
    
    all_accounts = [account for account in all_accounts if verify_email(account['clientInfo']['email'])]
    
    if tm.chunk_size == 1:
        return all_accounts
    try:
        all_accounts = sorted(all_accounts, key=lambda k: datetime.fromtimestamp(int(k['clientInfo']['registrationDate'])/1000))
    except Exception as e:
        print('error sorting accounts')
    all_accounts =  divide_list(all_accounts, tm.chunk_size)[tm.chunk_number]
    
    
    print(
        'all_accounts: ' + str(len(all_accounts)),
        'time' + str(datetime.now() - datemilliseconds)
    )
    return all_accounts

def get_broker_accounts_all_filter(tm):

    all_accounts = tm.broker_manager.get_all_accounts(1,  0)
    all_accounts = [account for account in all_accounts if verify_email(account['clientInfo']['email'])]
    
    
    try:
        all_accounts = sorted(all_accounts, key=lambda k: datetime.fromtimestamp(int(k['clientInfo']['registrationDate'])/1000))
    except Exception as e:
        print('error sorting accounts')

    #filter accounts date before 10 minutes ago
    return  [account for account in all_accounts if datetime.fromtimestamp(int(account['clientInfo']['registrationDate'])/1000) < datetime.now() - timedelta(minutes=15)]

def get_broker_accounts_without_todays_updated_at_daily_drawdown(broker_accounts):
    
    ''''
        Get yesterday's balances by broker accounts 
    '''
    today_at_0000 = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    mt_user_ids = list(TradingAccount().db.find({
        'mt_user_id': {'$in': [account['clientId'] for account in broker_accounts]},
        'updated_at_daily_drawdown': {
            '$gte': today_at_0000
        }
    }).distinct('mt_user_id'))
    
    # retornamos broker_accounts que esten en mt_user_ids
    return [account for account in broker_accounts if account['clientId'] not in mt_user_ids]

def get_broker_accounts_chunked_filter(tm):
    '''
        Get all broker accounts chunked,
        obtain the chunk size from the environment variable CHUNK_SIZE
        obtain the chunk number from the environment variable CHUNK_NUMBER
    '''
    datemilliseconds = datetime.now()
    
    all_accounts = get_broker_accounts_all_filter(tm)
    
    all_accounts =  divide_list(all_accounts, tm.chunk_size)[tm.chunk_number]
    
    print(
        'all_accounts: ' + str(len(all_accounts)),
        'time' + str(datetime.now() - datemilliseconds)
    )
    return all_accounts


def get_broker_accounts_chunked_set(tm):
    '''
        Get all broker accounts chunked,
        obtain the chunk size from the environment variable CHUNK_SIZE
        obtain the chunk number from the environment variable CHUNK_NUMBER
        return a set of mt_user_id + '_' + broker
    '''
    chucked_accounts = get_broker_accounts_chunked(tm)

    return broker_accounts_to_set(chucked_accounts)

# USERS


def user_to_set_login_name(users):
    '''
        Convert users to a set
    '''
    users_data = {}
    for user in users:
        users_data[user['Login_name']] = user

    return users_data


def users_to_set(users):
    '''
        Convert users to a set
    '''
    users_data = {}
    for user in users:
        users_data[str(user['_id'])] = user

    return users_data


def get_chunked_users(tm):
    '''
        Get all users chunked,
        obtain the chunk size from the environment variable CHUNK_SIZE
        obtain the chunk number from the environment variable CHUNK_NUMBER
    '''
    filter = {
        'Login_name': {
            '$not': {
                '$regex': 'cryptofundtrader.com'
            }
        }
    }
    if os.getenv('TEST_ENVIRONMENT', 'False') == 'True':
        filter = {
            'Login_name': {
                '$regex': 'cryptofundtrader.com'
            }
        }

    total_users = User().db.collection.count_documents(filter)
    limit = int(total_users / tm.chunk_size)
    users_db = list(User().db.collection.find(filter).limit(limit).skip(limit * tm.chunk_number ))

    return users_db


def get_user_by_trading_accounts(trading_accounts):
    '''
        Get all users by trading accounts
    '''
    users_in_db = list(User().db.find({
        '_id': {"$in": [ta['user_id'] for ta in trading_accounts]}
    }))
    return users_in_db


def get_user_by_trading_accounts_set(trading_accounts):
    '''
        Get all users by trading accounts
    '''
    users_in_db = get_user_by_trading_accounts(trading_accounts)

    return users_to_set(users_in_db)


def get_user_by_trading_accounts_login_name_set(trading_accounts):
    '''
        Get all users by trading accounts
    '''
    users_in_db = get_user_by_trading_accounts(trading_accounts)

    return user_to_set_login_name(users_in_db)


def get_users_by_broker_accounts(broker_accounts):

    emails = [account['clientInfo']['email'] for account in broker_accounts]
    users_in_db = list(User().db.find({
        'User_email': {"$in": emails}
    }))
    return users_in_db


def get_users_by_broker_accounts_set(broker_accounts):
    '''
        Get all users by broker accounts
    '''
    users_in_db = get_users_by_broker_accounts(broker_accounts)

    return users_to_set(users_in_db)


def get_users_by_broker_accounts_login_name_set(broker_accounts):
    '''
        Get all users by broker accounts
    '''
    users_in_db = get_users_by_broker_accounts(broker_accounts)

    return user_to_set_login_name(users_in_db)

# def get_uers_

# TRADING ACCOUNTS

def users_set_to_trading_accounts_set():
    tranding_accounts = list(TradingAccount().db.collection.find({}, {'mt_user_id': 1, 'user_id': 1}))
    result = {}
    for ta in tranding_accounts:
        result[ta['mt_user_id']] = ta['user_id']
    return result

def get_trading_accounts_by_trades_only_mt_user_id_and_balance(mt_user_ids):
    tranding_accounts = list(TradingAccount().db.collection.find({
        'mt_user_id': {'$in': mt_user_ids},
        }, {'mt_user_id': 1, 'user_id': 1, 'balance': 1, 'remarks': 1}))
    result = {}
    for ta in tranding_accounts:
        result[ta['mt_user_id']] = {
            'user_id': ta['user_id'],
            'balance': ta['balance'],
            'mt_user_id': ta['mt_user_id'],
            'remarks': ta['remarks']
        
            }
    return result
    

def trading_account_to_set(trading_accounts):
    '''
        Convert trading accounts to a set, key 
    '''
    trading_accounts_data = {}
    for ta in trading_accounts:
        trading_accounts_data[ta['mt_user_id'] + '_' + ta['broker']] = ta

    return trading_accounts_data


def get_trading_accounts_by_users(users):
    '''
        Get all trading accounts by chunked users
    '''
    trading_accounts_db = list(TradingAccount().db.find({
        'user_id': {'$in': [user['_id'] for user in users]}
    }))
    return trading_accounts_db


def get_all_trading_accounts_by_broker_accounts(broker_accounts):
    '''
        Get all trading accounts by broker accounts
    '''
    trading_accounts_db = list(TradingAccount().db.find({
        'mt_user_id': {'$in': [account['clientId'] for account in broker_accounts]}
    }))
    return trading_accounts_db


def get_all_trading_accounts_by_offending_accounts(offending_accounts):
    '''
        Get all trading accounts by broker accounts
    '''
    trading_accounts_db = list(TradingAccount().db.find({
        'mt_user_id': {'$in': [account['mt_user_id'] for account in offending_accounts], '$nin': ['123456']},
    }))
    return trading_accounts_db


def get_trading_accounts_by_broker_accounts_active_pending_withdrawal(broker_accounts):
    '''
        Get all trading accounts by broker accounts
    '''
    trading_accounts_db = list(TradingAccount().db.find({
        'mt_user_id': {'$in': [account['clientId'] for account in broker_accounts]},
        'status': {
            '$in': ['active', 'pending_withdrawal']
        }
    }))
    return trading_accounts_db


def get_trading_accounts_by_chunked_users_active_pending_withdrawal(tm):
    '''
        Get all trading accounts by broker accounts
    '''
    users_db = get_chunked_users(tm)
    trading_accounts_db = list(TradingAccount().db.find({
        'user_id': {'$in': [str(user['_id']) for user in users_db]},
        'status': {
            '$in': ['active', 'pending_withdrawal']
        }
    }))
    return trading_accounts_db


def get_trading_accounts_by_chunked_users_active_pending_withdrawal_set(tm):
    '''
        Get all trading accounts by broker accounts
    '''
    trading_accounts_db = get_trading_accounts_by_chunked_users_active_pending_withdrawal(
        tm)

    return trading_account_to_set(trading_accounts_db)


def get_trading_accounts_by_broker_accounts_active_pending_withdrawal_set(broker_accounts):
    '''
        Get all trading accounts by broker accounts
    '''
    trading_accounts_db = get_trading_accounts_by_broker_accounts_active_pending_withdrawal(
        broker_accounts)

    return trading_account_to_set(trading_accounts_db)


def get_all_trading_accounts_by_broker_accounts_set(broker_accounts):
    '''
        Get all trading accounts by broker accounts
    '''
    trading_accounts_db = get_all_trading_accounts_by_broker_accounts(
        broker_accounts)

    return trading_account_to_set(trading_accounts_db)

# TRADES


def trades_to_set(grouped_trades):
    '''
        Convert trades to a set, key is mt_user_id + '_' + broker
    '''
    trades_data = {}
    for g_trade in grouped_trades:
        trade = g_trade['_id']
        total_trades = g_trade['total_trades']
        try:
            trades_data[trade['mt_user_id'] + '_' + trade['broker']] = total_trades
            
        except Exception as e:
            print('## falla ## ')
            print(trade)
            return
    return trades_data


def trades_to_set_trader_id(trades):
    '''
        Convert trades to a set, key is trader_id
    '''
    trades_data = {}
    for trade in trades:

        trades_data[trade['trader_id']] = trade

    return trades_data

def trades_to_set_array(trades):
    '''
        Convert trades to a set, key is mt_user_id + '_' + broker
    '''
    trades_data = {}
    for trade in trades:
        if trade['mt_user_id'] + '_' + trade['broker'] in trades_data:
            trades_data[trade['mt_user_id'] + '_' + trade['broker']].append(trade)
        else:
            trades_data[trade['mt_user_id'] + '_' + trade['broker']] = [trade]

    return trades_data

def get_all_trades_query(tm, match):
    '''
        Get all trades query
    '''
    all_trades = list(Trade().db.collection.aggregate([
        {
            '$match': match
        },
        {
            '$group': {
                '_id': {
                    'mt_user_id': '$mt_user_id',
                    'broker': '$broker'
                },
                'total_trades': {'$sum': 1}
            }
        }
    ]))

    return all_trades


def get_all_trades_count_by_broker_accounts(tm, broker_accounts):
    '''
        Get all trades count by mt_user_id
    '''
    all_trades_count_by_mt_user_id = get_all_trades_query(tm, {
        'mt_user_id': {
            '$in': [account['clientId'] for account in broker_accounts]
        },
        # 'status': 'active'
    })
    return all_trades_count_by_mt_user_id


def get_last_trades_count_by_broker_accounts_set(tm, broker_accounts):
    '''
        Get all trades count by mt_user_id
    '''
    all_trades_count_by_mt_user_id = get_all_trades_count_by_broker_accounts(
        tm, broker_accounts)

    return trades_to_set(all_trades_count_by_mt_user_id)


def get_yesterday_trades_count_by_broker_accounts(tm, broker_accounts):
    '''
        Get yesterday all trades count by mt_user_id
    '''
    yesterday = datetime.now() - timedelta(days=1)
    yesterday = yesterday.strftime("%m/%d/%Y")

    all_trades_count_by_mt_user_id = get_all_trades_query(tm, {
        'mt_user_id': {
            '$in': [account['clientId'] for account in broker_accounts]
        },
        'close_time': {"$eq": yesterday},
        # 'status': 'active'
    })
    return all_trades_count_by_mt_user_id


def get_yesterday_trades_count_by_broker_accounts_set(tm, broker_accounts):
    '''
        Get yesterday all trades count by mt_user_id
    '''
    all_trades_count_by_mt_user_id = get_yesterday_trades_count_by_broker_accounts(
        tm, broker_accounts)

    return trades_to_set(all_trades_count_by_mt_user_id)


def get_trades_actives_by_broker_accounts(broker_accounts):
    '''
        Get all trades count by mt_user_id
    '''
    all_trades_count_by_mt_user_id = Trade().db.collection.find({
        'mt_user_id': {
            '$in': [account['clientId'] for account in broker_accounts]
        },
        'status': 'active'
    })
    return all_trades_count_by_mt_user_id


def get_trades_actives_by_broker_accounts_set(broker_accounts):
    '''
        Get all trades count by mt_user_id
    '''
    all_trades_count_by_mt_user_id = get_trades_actives_by_broker_accounts(
        broker_accounts)

    return trades_to_set_array(all_trades_count_by_mt_user_id)

def get_trades_actives_by_trading_accounts(trading_accounts):
    '''
        Get all trades count by mt_user_id
    '''
    all_trades_count_by_mt_user_id = Trade().db.collection.find({
        'mt_user_id': {
            '$in': [account['mt_user_id'] for account in trading_accounts]
        },
        'status': 'active'
    })
    return all_trades_count_by_mt_user_id

def get_trades_actives_by_trading_accounts_set(trading_accounts):
    '''
        Get all trades count by mt_user_id
    '''
    all_trades_count_by_mt_user_id = get_trades_actives_by_trading_accounts(
        trading_accounts)

    return trades_to_set_array(all_trades_count_by_mt_user_id)

def get_trades_by_trader_ids(trader_ids):
    trades = list(Trade().db.find({
        'trader_id': {
            '$in': trader_ids
        }
    }))
    
    return trades
    
def get_trades_by_trader_ids_set(trader_ids):
    '''
        Get all trades by trader ids and convert to set key is mt_user_id + '_' + broker
    '''
    all_trades_count_by_mt_user_id = get_trades_by_trader_ids(
        trader_ids)

    return trades_to_set_trader_id(all_trades_count_by_mt_user_id)

# BALANCES


def balances_to_set(balances):
    '''
        Convert balances to a set, key is mt_user_id + '_' + date_year_month_day

    '''
    balances_data = {}
    for balance in balances:
        balances_data[balance['mt_user_id'] + '_' +
                      balance['date_year_month_day']] = balance

    return balances_data


def get_balances_by_broker_accounts_and_date(broker_accounts, date_year_month_day):
    '''
        Get all balances by broker accounts and date
    '''
    balances = list(Balance().db.find({
        'mt_user_id': {'$in': [account['clientId'] for account in broker_accounts]},
        'date_year_month_day': date_year_month_day
    }))
    return balances


def get_today_balances_by_broker_accounts(broker_accounts):
    ''''
        Get today's balances by broker accounts 
    '''
    date_year_month_day = datetime.utcnow().strftime('%Y-%m-%d')
    balances = get_balances_by_broker_accounts_and_date(
        broker_accounts, date_year_month_day)
    return balances


def get_today_balances_by_broker_accounts_set(broker_accounts):
    ''''
        Get today's balances by broker accounts 
    '''
    balances = get_today_balances_by_broker_accounts(broker_accounts)

    return balances_to_set(balances)


def get_yesterday_balances_by_broker_accounts(broker_accounts):
    ''''
        Get yesterday's balances by broker accounts 
    '''
    date_year_month_day = (datetime.utcnow() -
                           timedelta(days=1)).strftime('%Y-%m-%d')
    balances = get_balances_by_broker_accounts_and_date(
        broker_accounts, date_year_month_day)
    return balances


def get_yesterday_balances_by_broker_accounts_set(broker_accounts):
    ''''
        Get yesterday's balances by broker accounts 
    '''
    balances = get_yesterday_balances_by_broker_accounts(broker_accounts)

    return balances_to_set(balances)

def get_broker_accounts_without_todays_balance(broker_accounts):
    ''''
        Get yesterday's balances by broker accounts 
    '''
    date_year_month_day = datetime.utcnow().strftime('%Y-%m-%d')
    mt_user_ids = list(Balance().db.find({
        'mt_user_id': {'$in': [account['clientId'] for account in broker_accounts]},
        'date_year_month_day': date_year_month_day
    }).distinct('mt_user_id'))
    
    # retornamos broker_accounts que esten en mt_user_ids
    return [account for account in broker_accounts if account['clientId'] not in mt_user_ids]

# REMARKS
def remarks_to_set(remarks):
    '''
        Convert remarks to a set
    '''
    remarks_data = {}
    for remark in remarks:
        remarks_data[remark['key']] = remark

    return remarks_data


def get_remarks_data():
    global remarks_internal_cache
    global remarks_last_update
    current_date = datetime.now()
    if current_date - remarks_last_update > timedelta(minutes=3600):
        print('updating remarks cache')
        remarks_internal_cache = list(Remarks().find({}))
        remarks_last_update = current_date
    return remarks_internal_cache


# WITHDRAWALS

def withdrawals_to_set(withdrawals):
    '''
        Convert withdrawals to a set
    '''
    withdrawals_data = {}
    for withdrawal in withdrawals:
        withdrawals_data[withdrawal['mt_user_id']] = withdrawal

    return withdrawals_data


def get_last_withdrawals_by_trading_accounts(tm, trading_accounts):
    pipeline = [
        {
            "$match": {
                "mt_user_id": {
                    # Convertimos cada ID a string antes de hacer el match.
                    "$in": [str(ta['mt_user_id']) for ta in trading_accounts]
                }
            }
        },
        {
            "$sort": {
                "created_at": -1  # Ordenamos por ID de forma descendente.
            }
        },
        {
            "$group": {
                "_id": "$mt_user_id",  # Agrupamos por mt_user_id.
                "last_withdrawal": {
                    # Nos quedamos con el primer documento de cada grupo, que será el retiro más reciente debido al orden previo.
                    "$first": "$$ROOT"
                }
            }
        },
        {
            "$replaceRoot": {  # Hacemos que el retiro más reciente sea el documento principal en lugar del documento agrupado.
                "newRoot": "$last_withdrawal"
            }
        }
    ]

    last_withdrawals_by_mt_user_id = list(
        tm.withdrawals.db.collection.aggregate(pipeline))
    return last_withdrawals_by_mt_user_id


def get_last_withdrawals_by_trading_accounts_set(tm, trading_accounts):
    '''
        Get last withdrawals by trading accounts
    '''
    last_withdrawals_by_mt_user_id = get_last_withdrawals_by_trading_accounts(
        tm, trading_accounts)
    return withdrawals_to_set(last_withdrawals_by_mt_user_id)


def print_chunk_data(tm, method):
    print(method)
    datemilliseconds = datetime.now()
    print('getting data ', datemilliseconds)
    print('chunk_size: ' + str(tm.chunk_size))
    print('chunk_number: ' + str(tm.chunk_number))
