import common_libraries.functions as functions
import common_libraries.utils as utils
import requests
import utils.cache_data_access as cda
import pymongo
import os
import sys
import time
import signal
from datetime import datetime, timedelta, date, timezone
import json
from common_libraries.classes.broker.manager import BrokerManager
from common_libraries.classes.database.trade import Trade
from common_libraries.classes.database.position import Position
from common_libraries.classes.database.trading_account import TradingAccount
from common_libraries.classes.database.user import User
from common_libraries.classes.database.remarks import Remarks
from common_libraries.classes.database.balance import Balance
from common_libraries.classes.database.withdrawal import Withdrawal
from common_libraries.classes.database.symbols import Symbols
from common_libraries.classes.database.database import Database

from bson.objectid import ObjectId

from pymongo import collection, collection, collection, MongoClient, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError
import redis


def send_telegram_message_with_title(title, message):

    if 'strong' not in title:
        title = '<strong>' + title + '</strong>'

    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    # url_with_format
    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={title}\n{message}&parse_mode=HTML"
    print(requests.get(url).json())  # this sends the message


class TaskManager():
    """
    Create class to manager all tasks
    - update_users
    - update_trades
    - update_wordpress_leaderboard
    - update_wordpress_tournament
    - update_users_statistics
    - update_all_symbols
    - update_pending_withdrawals
    - check_trades_too_fast
    """

    def __init__(self) -> None:
        self.broker_manager = BrokerManager()
        self.users = User()
        self.trades = Trade()
        self.positions = Position()
        self.trading_accounts = TradingAccount()
        self.remarks = Remarks()
        self.balances = Balance()
        self.withdrawals = Withdrawal()
        self.symbols = Symbols()
        self.remarksDB = cda.get_remarks_data()
        self.wordpress_task = True
        self.chunk_number = 0
        self.chunk_size = 1
        self.send_n8n_notifications = True
        self.close_accounts = True
        self.telegrams_prefix = ''
        try:
            self.chunk_number = int(os.getenv('CHUNK_NUMBER', "0"))
            self.chunk_size = int(os.getenv('CHUNK_SIZE', "1"))
            self.send_n8n_notifications = os.getenv(
                "SEND_N8N_NOTIFICATIONS", "True") == "True"
            self.close_accounts = os.getenv(
                "CLOSE_ACCOUNTS", "False") == "True"
            self.wordpress_task = os.getenv(
                "WORDPRESS_TASK", "False") == "True"
            self.telegrams_prefix = os.getenv("TELEGRAM_PREFIX", "")

        except Exception as e:
            print(e)

    def get_account_by_email(self):
        account = self.broker_manager.get_account_by_email(
            "mcolomer@killia.com", "matchtrader")
        print(account)

    @utils.task_log('backup_all_broker_accounts')
    def backup_all_broker_accounts(self):
        broker_accounts_to_backup = []
        broker_accounts = cda.get_broker_accounts_chunked(self)

        db = Database('broker_accounts').collection
        broker_accounts_db = list(db.collection.find({
            'mt_user_id': {"$in": [account['clientId'] for account in broker_accounts]}
        }))
        broker_accounts_db_set = {}
        for account in broker_accounts_db:
            broker_accounts_db_set[account['mt_user_id']] = account

        for account in broker_accounts:
            broker_account = broker_accounts_db_set.get(
                account['clientId'], None)
            if broker_account is None:
                data = {
                    'mt_user_id': account['clientId'],
                    'broker': account['broker'],
                    'data': account,
                }
                broker_accounts_to_backup.append(InsertOne(data))
            else:
                broker_accounts_to_backup.append(
                    UpdateOne({'_id': broker_account['_id']}, {'$set': {'data': account}}))

        if len(broker_accounts_to_backup) > 0:
            try:
                db.bulk_write(broker_accounts_to_backup, ordered=False)
            except BulkWriteError as bwe:
                print(bwe.details)

    @utils.task_log('update_all_symbols')
    def update_all_symbols(self):

        if self.chunk_number == 1:
            symbols_to_create = []
            symbols = self.broker_manager.get_all_symbols()
            all_symbols = list(Symbols().collection.find({}))
            all_symbols_set = {}
            for symbol in all_symbols:
                all_symbols_set[symbol['symbol'] +
                                '_' + symbol['broker']] = symbol

            for symbol in symbols:
                # print(symbol)
                symbol_obj = all_symbols_set.get(
                    symbol['symbol'] + '_' + symbol['broker'], None)
                if symbol_obj is None:
                    symbol['created_at'] = datetime.now()
                    symbols_to_create.append(InsertOne(symbol))
            if len(symbols_to_create) > 0:
                try:
                    Symbols().collection.bulk_write(symbols_to_create, ordered=False)
                except BulkWriteError as bwe:
                    print(bwe.details)

    @utils.task_log('update_users')
    def update_users(self):
        self.update_users_collection()
        self.update_positions_collection()
        # self.check_hedging_rule_for_all_accounts()
        self.check_targets_and_update()

    def is_hedging_rule_supported(self, broker):
        return self.broker_manager.is_hedging_rule_supported(broker)

    def is_crypto_instrument(self, instrument, broker):
        instrument = instrument.replace('USD', '').replace('EUR', '')
        return self.broker_manager.is_supported_instrument(instrument, broker)

    def notify_30seconds_rule(self, trading_account, details={}):
        if not self.send_n8n_notifications:
            return False

        if trading_account is not None:
            user = self.users.find_one_by_id(trading_account['user_id'])
            # self.close_current_account(user, trading_account, '30seconds_rule_detected')
            # notify to n8n
            if user is not None:
                url = "https://n8n.cryptofundtrader.com/webhook/rule-detected"
                headers = {
                    'Content-Type': 'application/json'
                }
                data = {
                    'environment': os.getenv('ENVIRONMENT'),
                    'user_id': str(user['_id']),
                    'email': utils.get_email_by_environment(user['User_email']),
                    'firstname': user['First_name'],
                    'lastname': user['last_name'],
                    'mt_user_ids': [trading_account['mt_user_id']],
                    'details': details,
                    'rule': '30 seconds',
                }

                print("Sending 30 seconds rule detected to n8n")

                response = utils.do_post_request(url, headers, data)
                print("Response: " + str(response))
                if response is not None and response.status_code == 200:
                    utils.print_with_module("TaskManager.detect_and_notify_30seconds_rule", "30 seconds rule detected for user " + str(
                        trading_account['user_id']) + " - " + str(trading_account['mt_user_id']))
                    return True
        else:
            print("Trading account not found")

        return False

    def detect_and_notify_30seconds_rule(self, trading_acount):
        if self.is_30seconds_rule_supported(trading_acount['broker']):
            # logic for 30 seconds rule
            is_30seconds_rule = self.is_30seconds_rule(trading_acount)
            if is_30seconds_rule:
                self.notify_30seconds_rule(trading_acount)
                return True

        return False

    def detect_hedging_rule_in_all_open_positions(self):
        # trading_accounts by open_positions
        brokers = self.broker_manager.get_supported_brokers()
        all_symbols = self.generate_symbols_by_brokers()

        current_time = time.time() * 1000
        last_5_minutes_time = current_time - (60 * 5 * 1000)
        last_24_hours_time = current_time - \
            (60 * 60 * 24 * 1000)  # 86400 = 1 day

        for broker in brokers:
            print("Checking hedging rule for broker " + broker)
            positions = self.positions.find_by_filter({'broker': broker})
            positions_count = self.positions.count_documents(
                {'broker': broker})
            positions_by_user_ids = {}
            offending_accounts = {}
            print("Positions count: " + str(positions_count) +
                  " for broker " + broker)
            for position in positions:
                local_broker = position['broker']
                user_id = str(position['user_id'])
                mt_user_id = str(position['mt_user_id'])
                ta = self.trading_accounts.find_one_by_mt_user_id_and_broker(
                    mt_user_id, local_broker)
                if ta is None:
                    print("Trading account not found for user " +
                          user_id + " and broker " + broker)
                if ta is not None and ta['status'] == 'active':
                    volume = int(position['position']['volume'])
                    buy_or_sell = 'buy' if volume > 0 else 'sell'

                    instrument = position['position']['instrument']
                    instrument = self.get_cleaned_symbol(
                        all_symbols[local_broker], instrument)
                    position['position']['openTime'] = int(
                        position['position']['openTime'])
                    is_time_ok = position['position']['openTime'] < last_5_minutes_time and position[
                        'position']['openTime'] > last_24_hours_time
                    if is_time_ok:
                        if user_id not in positions_by_user_ids:
                            positions_by_user_ids[user_id] = {}

                        if local_broker not in positions_by_user_ids[user_id]:
                            positions_by_user_ids[user_id][local_broker] = {}

                        if mt_user_id not in positions_by_user_ids[user_id][local_broker]:
                            positions_by_user_ids[user_id][local_broker][mt_user_id] = {
                            }

                        if instrument not in positions_by_user_ids[user_id][local_broker][mt_user_id]:
                            positions_by_user_ids[user_id][local_broker][mt_user_id][instrument] = {
                            }

                        keys = list(
                            positions_by_user_ids[user_id][local_broker][mt_user_id][instrument].keys())
                        if buy_or_sell not in keys:
                            positions_by_user_ids[user_id][local_broker][mt_user_id][instrument][buy_or_sell] = 1
                        else:
                            positions_by_user_ids[user_id][local_broker][mt_user_id][instrument][buy_or_sell] += 1

                        if 'buy' in keys and 'sell' in keys:
                            # print ('hedging rule detected for user ' + str(user_id) + ' - ' + str(mt_user_id) + ' - ' + instrument + ' in local_broker ' + local_broker)
                            positions_by_user_ids[user_id][local_broker][mt_user_id][instrument]['detected'] = True
                            if user_id not in offending_accounts:
                                offending_accounts[user_id] = []

                            # find if mt_user_id and instrument already exists in offending_accounts
                            found = False
                            if len(offending_accounts[user_id]) > 0:
                                for account in offending_accounts[user_id]:
                                    if account['mt_user_id'] == mt_user_id and account['instrument'] == instrument and account['broker'] == local_broker:
                                        found = True
                                        break

                            if found is False:
                                offending_accounts[user_id].append({
                                    'mt_user_id': mt_user_id,
                                    'instrument': instrument,
                                    'broker': local_broker
                                })

            if len(offending_accounts) > 0:
                for user_id in offending_accounts:
                    # print ("User_id: " + str(user_id) + " with the next offending accounts: " + str(offending_accounts[user_id]))
                    pass

                utils.print_with_module("TaskManager.detect_hedging_rule_in_all_open_positions", "Hedging rule detected for broker " +
                                        broker + " for " + str(len(offending_accounts)) + " users", 'offending_accounts', offending_accounts)

        return offending_accounts

    def detect_hedging_rule_in_one_account(self, trading_account_id):
        # trading_accounts by open_positions
        tas = []
        ta = self.trading_accounts.find_one_by_mt_user_id(
            str(trading_account_id))
        if ta is not None:
            tas = [ta]

        final_detected_accounts = {}
        for ta in tas:
            print("Checking hedging rule for trading account " +
                  str(ta['mt_user_id']) + " and broker " + ta['broker'])
            detected_accounts = self.detect_hedging_rules_in_account(ta)
            if len(detected_accounts.keys()) > 0:
                print("Hedging rule detected for user " +
                      str(ta['user_id']) + " with the next accounts: " + str(detected_accounts))
                # merge detected_accounts with final_detected_accounts
                for user_id in detected_accounts:
                    if user_id not in final_detected_accounts:
                        final_detected_accounts[user_id] = []

                    for account in detected_accounts[user_id]:
                        found = False
                        for final_account in final_detected_accounts[user_id]:
                            if str(final_account['mt_user_id']) == str(account['mt_user_id']) and str(final_account['instrument']) == str(account['instrument']) and final_account['broker'] == account['broker']:
                                found = True
                                break

                        if found is False:
                            final_detected_accounts[user_id].append(account)

        return final_detected_accounts

    def get_unique_trading_accounts_by_last_n_hours_and_trades(self, hours_ago=24, number_of_trades=2):
        '''
        Get unique trading accounts by last n hours and number of trades
        return list of trading accounts
        '''
        created_at = datetime.now() - timedelta(hours=hours_ago)
        trading_accounts_total = self.trading_accounts.count_documents({
            'status': {'$ne': 'closed'},
        })
        limit = int(trading_accounts_total / self.chunk_size)

        trading_accounts = list(self.trading_accounts.db.collection.find({
            'status': {
                '$ne': 'closed'
            }
        }).limit(limit).skip(limit * self.chunk_number))

        filter = {
            'status': 'active',
            'created_at': {
                '$gte': created_at
            },
            'mt_user_id': {
                '$in': [ta['mt_user_id'] for ta in trading_accounts]
            }
        }
        print("Filter: " + str(filter), created_at)
        trades = self.trades.db.collection.find(filter)
        trades_list = list(trades)
        print(trades_list)
        print("Found " + str(len(trades_list)) +
              " trades in the last " + str(hours_ago) + " hours")

        trading_accounts = {}
        for trade in trades_list:
            id = str(trade['mt_user_id']) + '_' + str(trade['broker'])
            if id not in trading_accounts:
                trading_accounts[id] = {
                    'mt_user_id': trade['mt_user_id'],
                    'broker': trade['broker'],
                    'trades': 1
                }
            else:
                trading_accounts[id]['trades'] += 1

        unique_trading_accounts = []
        for id in trading_accounts:
            if trading_accounts[id]['trades'] >= number_of_trades:
                unique_trading_accounts.append(trading_accounts[id])

        # map only mt_user_id and broker
        unique_trading_accounts = list(map(lambda x: {
                                       'mt_user_id': x['mt_user_id'], 'broker': x['broker']}, unique_trading_accounts))

        return unique_trading_accounts

    def detect_hedging_rule_in_all_accounts(self):
        try:
            # trading_accounts by open_positions
            start_time = time.time()
            list_of_trading_accounts = self.get_unique_trading_accounts_by_last_n_hours_and_trades(
                24, 2)
            # get first 10 accounts
            # list_of_trading_accounts = list_of_trading_accounts[0:10]
            final_detected_accounts = []
            if (len(list_of_trading_accounts) > 0):
                print("Limited trading accounts found: " +
                      str(len(list_of_trading_accounts)))
                # tas = self.trading_accounts.find_by_status('active')
                # #show total
                # print ("Total trading accounts found: " + str(len(list(tas))))
                tas = list(self.trading_accounts.find_active_by_mt_user_ids_and_brokers(
                    list_of_trading_accounts))
                # show total tas found
                print("Filtered trading accounts found: " + str(len(tas)))

                processed_trading_accounts = []
                for ta in tas:
                    key = ta['mt_user_id'] + '_' + ta['broker']
                    if key not in processed_trading_accounts:
                        processed_trading_accounts.append(key)
                        print("Checking hedging rule for trading account " +
                              str(ta['mt_user_id']) + " and broker " + ta['broker'])
                        detected_rule = self.detect_hedging_rule_in_account(ta)
                        keys = detected_rule.keys()
                        print("Keys: " + str(keys))
                        if len(keys) > 0 and ta['mt_user_id'] in keys and len(detected_rule[ta['mt_user_id']].keys()) > 0:
                            print("Hedging rule detected for user " +
                                  str(ta['user_id']) + " with the next data: " + str(detected_rule))
                            # merge detected_rule with final_detected_accounts
                            final_detected_accounts.append({
                                'user_id': str(ta['user_id']),
                                'mt_user_id': ta['mt_user_id'],
                                'broker': ta['broker'],
                                'detected_rules': detected_rule[ta['mt_user_id']]
                            })
                        else:
                            print("No hedging rule detected for trading account " +
                                  str(ta['mt_user_id']) + " and broker " + ta['broker'])

            print("Time elapsed: " + str(time.time() - start_time))

            print("Total detected accounts: " +
                  str(len(final_detected_accounts)))

            return final_detected_accounts
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print(e)

    def get_shared_milliseconds(self, start_time_1, end_time_1, start_time_2, end_time_2):
        if start_time_1 > end_time_2 or start_time_2 > end_time_1:
            return 0

        if start_time_1 > start_time_2:
            start_time = start_time_1
        else:
            start_time = start_time_2

        if end_time_1 > end_time_2:
            end_time = end_time_2
        else:
            end_time = end_time_1

        return end_time - start_time

    def detect_hedging_rule_in_account(self, trading_account):
        try:
            broker = trading_account['broker'] if 'broker' in trading_account else 'matchtrader'

            hedging_rule_detected = False
            current_time = time.time() * 1000
            last_2_minutes_time = current_time - (60 * 2 * 1000)
            last_24_hours_time = current_time - \
                (60 * 60 * 24 * 1000)  # 86400 = 1 day
            last_24_hours_created_at = datetime.fromtimestamp(
                last_24_hours_time / 1000.0)

            if self.is_hedging_rule_supported(broker):
                # recover all trading_accounts by user id
                seconds_in_a_day = 86400
                trades_by_mt_user_ids = {}
                if trading_account['status'] == 'active':
                    broker = trading_account['broker'] if 'broker' in trading_account else 'matchtrader'
                    filter = {'mt_user_id': trading_account['mt_user_id'], 'broker': broker, 'status': 'active', 'created_at': {
                        '$gte': last_24_hours_created_at}}
                    trades = self.trades.find_by_filter(filter)
                    trades = list(trades)
                    print("total trades: " + str(len(trades)) + " for mt_user_id: " +
                          str(trading_account['mt_user_id']) + " and broker: " + broker)
                    if len(trades) > 1:
                        trades_out_of_time = 0
                        for trade in trades:
                            # print (trade)
                            if 'open_time_original' in trade and 'close_time_original' in trade:
                                cloned_trades = trades.copy()
                                is_trade_to_consider = False
                                for cloned_trade in cloned_trades:
                                    if cloned_trade['trader_id'] != trade['trader_id']:
                                        if (trade['direction'] == 'buy' and cloned_trade['direction'] == 'sell') or (trade['direction'] == 'sell' and cloned_trade['direction'] == 'buy'):
                                            original_trade_cleaned_instrument = self.get_simple_cleaned_symbol(
                                                trade['symbol'])
                                            clone_trade_cleaned_instrument = self.get_simple_cleaned_symbol(
                                                cloned_trade['symbol'])
                                            # print ("original: " + trade['symbol'] + "=>" + original_trade_cleaned_instrument)
                                            # print ("cloned: " + cloned_trade['symbol'] + "=>" + clone_trade_cleaned_instrument)

                                            if original_trade_cleaned_instrument == clone_trade_cleaned_instrument:
                                                # shared seconds
                                                if 'open_time_original' in cloned_trade and 'close_time_original' in cloned_trade:
                                                    shared_seconds = self.get_shared_milliseconds(
                                                        trade['open_time_original'], trade['close_time_original'], cloned_trade['open_time_original'], cloned_trade['close_time_original']) / 1000.0
                                                    print("trade original id " + str(trade['trader_id']) + " and cloned_trade original id " + str(
                                                        cloned_trade['trader_id']) + " have " + str(shared_seconds) + " seconds in common")
                                                    # diff seconds between trade open_time_original and cloned_trade open_time_original
                                                    diff_seconds = (
                                                        trade['open_time_original'] - cloned_trade['open_time_original']) / 1000.0
                                                    if shared_seconds >= 60 and diff_seconds <= seconds_in_a_day:
                                                        print("trade original id " + str(trade['trader_id']) + " and cloned_trade original id " + str(
                                                            cloned_trade['trader_id']) + " have " + str(shared_seconds) + " seconds in common")
                                                        is_trade_to_consider = True
                                                        break

                                if is_trade_to_consider is True:
                                    # calculate duration in seconds
                                    # seconds
                                    duration = (
                                        trade['close_time_original'] - trade['open_time_original']) / 1000

                                    # if openTime is older than 5 minutes and lower than 24 hours
                                    is_time_ok = trade['close_time_original'] < last_2_minutes_time and trade[
                                        'close_time_original'] > last_24_hours_time and duration >= 60
                                    print("is_time_ok", is_time_ok)

                                    print("trade['close_time_original']",
                                          trade['close_time_original'])
                                    print("last_24_hours_time",
                                          last_24_hours_time)
                                    print("last_2_minutes_time",
                                          last_2_minutes_time)
                                    trades_out_of_time += 1 if is_time_ok is False else 0
                                    if is_time_ok:
                                        mt_user_id = trade['mt_user_id']
                                        buy_or_sell = 'buy' if trade['lots'] > 0 else 'sell'

                                        instrument = self.get_simple_cleaned_symbol(
                                            trade['symbol'])

                                        if mt_user_id not in trades_by_mt_user_ids:
                                            trades_by_mt_user_ids[mt_user_id] = {
                                            }

                                        if instrument not in trades_by_mt_user_ids[mt_user_id]:
                                            trades_by_mt_user_ids[mt_user_id][instrument] = {
                                                'trades_buy': [],
                                                'trades_sell': [],
                                                'detected': False,
                                            }

                                        if buy_or_sell not in trades_by_mt_user_ids[mt_user_id][instrument].keys():
                                            trades_by_mt_user_ids[mt_user_id][instrument][buy_or_sell] = 1
                                        else:
                                            trades_by_mt_user_ids[mt_user_id][instrument][buy_or_sell] += 1

                                        # add trade id to list
                                        if 'broker_trade_id' in trade:
                                            trades_by_mt_user_ids[mt_user_id][instrument]['trades_' + buy_or_sell].append(
                                                trade['broker_trade_id'])

                                        keys = list(
                                            trades_by_mt_user_ids[mt_user_id][instrument].keys())
                                        if 'buy' in keys and 'sell' in keys:
                                            hedging_rule_detected = True
                                            trades_by_mt_user_ids[mt_user_id][instrument]['detected'] = True

                        if trades_out_of_time > 0:
                            utils.print_with_module("TaskManager.get_hedging_rules_by_trading_account", "Found " + str(
                                trades_out_of_time) + " trades out of time for mt_user_id: " + str(trading_account['mt_user_id']), 'mt_user_id', trading_account['mt_user_id'])
                # else:
                #     utils.print_with_module("TaskManager.get_hedging_rules_by_trading_account", "Trading account " + str(ta['mt_user_id']) + " is not active", 'mt_user_id', ta['mt_user_id'])

            if hedging_rule_detected:
                utils.print_with_module("TaskManager.get_hedging_rules_by_trading_account", "Hedging rule detected for user " + str(
                    trading_account['user_id']) + " - " + str(trades_by_mt_user_ids), 'mt_user_id', trading_account['mt_user_id'])

            # sample output:
            # {'306822': {'BTC': {'trades_buy': ['1688803028912324399', '1688803028912368028'], 'trades_sell': ['1688803028971727275'], 'detected': True, 'buy': 2, 'sell': 1}, 'ETH': {'trades_buy': ['1688803028912344024', '1688803028919566177', '1688803028926019106', '1688803028948757636'], 'trades_sell': [], 'detected': False, 'buy': 4}, 'ETC': {'trades_buy': ['1688803028914995808'], 'trades_sell': ['1688803028971727225'], 'detected': True, 'buy': 1, 'sell': 1}, 'AVE': {'trades_buy': [], 'trades_sell': ['1688803028971727216'], 'detected': False, 'sell': 1}, 'BNB': {'trades_buy': [], 'trades_sell': ['1688803028971727220'], 'detected': False, 'sell': 1}, 'SOL': {'trades_buy': [], 'trades_sell': ['1688803028971727269'], 'detected': False, 'sell': 1}}}
            # get only entries with instrument.detected = True
            filtered_detected_trades_by_mt_user_ids = {}
            for mt_user_id in trades_by_mt_user_ids.keys():
                filtered_detected_trades_by_mt_user_ids[mt_user_id] = {}
                for instrument in trades_by_mt_user_ids[mt_user_id].keys():
                    if trades_by_mt_user_ids[mt_user_id][instrument]['detected'] is True:
                        filtered_detected_trades_by_mt_user_ids[mt_user_id][
                            instrument] = trades_by_mt_user_ids[mt_user_id][instrument]

            detected_account_ids = filtered_detected_trades_by_mt_user_ids.keys()
            print("Detected accounts: ", trades_by_mt_user_ids)
            if len(detected_account_ids) > 0:
                print('filtered_detected_trades_by_mt_user_ids',
                      filtered_detected_trades_by_mt_user_ids)
                return filtered_detected_trades_by_mt_user_ids
            else:
                return {}

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print(e)

    def detect_hedging_rules_in_account(self, trading_account, only_detected_rule=True):
        broker = trading_account['broker'] if 'broker' in trading_account else 'matchtrader'

        hedging_rule_detected = False
        current_time = time.time() * 1000
        last_2_minutes_time = current_time - (60 * 2 * 1000)
        last_24_hours_time = current_time - \
            (60 * 60 * 24 * 1000)  # 86400 = 1 day
        last_24_hours_created_at = datetime.fromtimestamp(
            last_24_hours_time / 1000.0)

        if self.is_hedging_rule_supported(broker):
            # recover all trading_accounts by user id
            tas = self.trading_accounts.find_by_user_id(
                trading_account['user_id'])
            tas_count = self.trading_accounts.count_documents(
                {'user_id': trading_account['user_id']})
            seconds_in_a_day = 86400
            # print("Found " + str(tas_count) + " trading accounts for user_id: " + str(trading_account['user_id']))
            trades_by_mt_user_ids = {}
            for ta in tas:
                # print ("ta mt_user_id: " + str(ta['mt_user_id']) + " and broker: " + broker + " and status: " + ta['status'])
                if ta['status'] == 'active':
                    broker = ta['broker'] if 'broker' in ta else 'matchtrader'
                    filter = {'mt_user_id': ta['mt_user_id'], 'broker': broker, 'status': 'active', 'created_at': {
                        '$gte': last_24_hours_created_at}}
                    trades = self.trades.find_by_filter(filter)
                    trades = list(trades)
                    if len(trades) > 1:
                        print("total trades: " + str(len(trades)) + " for mt_user_id: " +
                              str(ta['mt_user_id']) + " and broker: " + broker)
                        trades_out_of_time = 0
                        for trade in trades:
                            # print (trade)
                            if 'open_time_original' in trade and 'close_time_original' in trade:
                                cloned_trades = trades.copy()
                                is_trade_to_consider = False
                                for cloned_trade in cloned_trades:
                                    if cloned_trade['trader_id'] != trade['trader_id']:
                                        if (trade['direction'] == 'buy' and cloned_trade['direction'] == 'sell') or (trade['direction'] == 'sell' and cloned_trade['direction'] == 'buy'):
                                            original_trade_cleaned_instrument = self.get_simple_cleaned_symbol(
                                                trade['symbol'])
                                            clone_trade_cleaned_instrument = self.get_simple_cleaned_symbol(
                                                cloned_trade['symbol'])
                                            # print ("original: " + trade['symbol'] + "=>" + original_trade_cleaned_instrument)
                                            # print ("cloned: " + cloned_trade['symbol'] + "=>" + clone_trade_cleaned_instrument)

                                            if original_trade_cleaned_instrument == clone_trade_cleaned_instrument:
                                                # shared seconds
                                                if 'open_time_original' in cloned_trade and 'close_time_original' in cloned_trade:
                                                    shared_seconds = self.get_shared_milliseconds(
                                                        trade['open_time_original'], trade['close_time_original'], cloned_trade['open_time_original'], cloned_trade['close_time_original']) / 1000.0
                                                    # diff seconds between trade open_time_original and cloned_trade open_time_original
                                                    diff_seconds = (
                                                        trade['open_time_original'] - cloned_trade['open_time_original']) / 1000.0
                                                    if shared_seconds >= 60 and diff_seconds <= seconds_in_a_day:
                                                        print("trade original id " + str(trade['trader_id']) + " and cloned_trade original id " + str(
                                                            cloned_trade['trader_id']) + " have " + str(shared_seconds) + " seconds in common")
                                                        is_trade_to_consider = True
                                                        break

                                print('##########3', is_trade_to_consider)
                                if is_trade_to_consider is True:
                                    # calculate duration in seconds
                                    # seconds
                                    duration = (
                                        trade['close_time_original'] - trade['open_time_original']) / 1000

                                    # if openTime is older than 5 minutes and lower than 24 hours
                                    # ahora 01/01/2023 16:00:00  => ahora 01/01/2023 15:58:00
                                    # 01/01/2023 15:00:00
                                    print("trade['close_time_original']",
                                          trade['close_time_original'])
                                    print("last_24_hours_time",
                                          last_24_hours_time)
                                    print("last_2_minutes_time",
                                          last_2_minutes_time)

                                    is_time_ok = trade['close_time_original'] < last_2_minutes_time and trade[
                                        'close_time_original'] > last_24_hours_time and duration >= 60

                                    trades_out_of_time += 1 if is_time_ok is False else 0
                                    if is_time_ok:
                                        mt_user_id = trade['mt_user_id']
                                        buy_or_sell = 'buy' if trade['lots'] > 0 else 'sell'

                                        instrument = self.get_simple_cleaned_symbol(
                                            trade['symbol'])

                                        if mt_user_id not in trades_by_mt_user_ids:
                                            trades_by_mt_user_ids[mt_user_id] = {
                                            }

                                        if instrument not in trades_by_mt_user_ids[mt_user_id]:
                                            trades_by_mt_user_ids[mt_user_id][instrument] = {
                                                'trades_buy': [],
                                                'trades_sell': [],
                                                'detected': False,
                                            }

                                        if buy_or_sell not in trades_by_mt_user_ids[mt_user_id][instrument].keys():
                                            trades_by_mt_user_ids[mt_user_id][instrument][buy_or_sell] = 1
                                        else:
                                            trades_by_mt_user_ids[mt_user_id][instrument][buy_or_sell] += 1

                                        # add trade id to list
                                        if 'broker_trade_id' in trade:
                                            trades_by_mt_user_ids[mt_user_id][instrument]['trades_' + buy_or_sell].append(
                                                trade['broker_trade_id'])

                                        keys = list(
                                            trades_by_mt_user_ids[mt_user_id][instrument].keys())
                                        if 'buy' in keys and 'sell' in keys:
                                            hedging_rule_detected = True
                                            trades_by_mt_user_ids[mt_user_id][instrument]['detected'] = True

                        if trades_out_of_time > 0:
                            utils.print_with_module("TaskManager.get_hedging_rules_by_trading_account", "Found " + str(
                                trades_out_of_time) + " trades out of time for mt_user_id: " + str(ta['mt_user_id']), 'mt_user_id', ta['mt_user_id'])
                # else:
                #     utils.print_with_module("TaskManager.get_hedging_rules_by_trading_account", "Trading account " + str(ta['mt_user_id']) + " is not active", 'mt_user_id', ta['mt_user_id'])

        if hedging_rule_detected:
            utils.print_with_module("TaskManager.get_hedging_rules_by_trading_account", "Hedging rule detected for user " + str(
                trading_account['user_id']) + " - " + str(trades_by_mt_user_ids), 'mt_user_id', trading_account['mt_user_id'])

        # sample output:
        # {'306822': {'BTC': {'trades_buy': ['1688803028912324399', '1688803028912368028'], 'trades_sell': ['1688803028971727275'], 'detected': True, 'buy': 2, 'sell': 1}, 'ETH': {'trades_buy': ['1688803028912344024', '1688803028919566177', '1688803028926019106', '1688803028948757636'], 'trades_sell': [], 'detected': False, 'buy': 4}, 'ETC': {'trades_buy': ['1688803028914995808'], 'trades_sell': ['1688803028971727225'], 'detected': True, 'buy': 1, 'sell': 1}, 'AVE': {'trades_buy': [], 'trades_sell': ['1688803028971727216'], 'detected': False, 'sell': 1}, 'BNB': {'trades_buy': [], 'trades_sell': ['1688803028971727220'], 'detected': False, 'sell': 1}, 'SOL': {'trades_buy': [], 'trades_sell': ['1688803028971727269'], 'detected': False, 'sell': 1}}}
        # get only entries with instrument.detected = True
        filtered_detected_trades_by_mt_user_ids = {}
        for mt_user_id in trades_by_mt_user_ids.keys():
            filtered_detected_trades_by_mt_user_ids[mt_user_id] = {}
            for instrument in trades_by_mt_user_ids[mt_user_id].keys():
                if trades_by_mt_user_ids[mt_user_id][instrument]['detected'] is True:
                    filtered_detected_trades_by_mt_user_ids[mt_user_id][
                        instrument] = trades_by_mt_user_ids[mt_user_id][instrument]

        detected_account_ids = filtered_detected_trades_by_mt_user_ids.keys()

        return trading_account['mt_user_id'] in detected_account_ids, filtered_detected_trades_by_mt_user_ids

    def is_hedging_rule(self, trading_account):
        return trading_account['mt_user_id'] in self.detect_hedging_rule_in_account(trading_account)

    def generate_symbols_by_brokers(self):
        symbols_by_brokers = {}
        for broker in self.broker_manager.get_supported_brokers():
            symbols_by_brokers[broker] = []
            instruments = self.symbols.find({'broker': broker})

            for instrument in instruments:
                data = {
                    'symbol': instrument['symbol'],
                    'cleaned': instrument['symbol_cleaned'],
                }
                symbols_by_brokers[broker].append(data)

        return symbols_by_brokers

    def is_30seconds_rule_supported(self, broker):
        return self.broker_manager.is_30seconds_rule_supported(broker)

    def is_30seconds_rule_when_withdrawal_allowed(self, ta, last_withdrawal, trades):

        diff_days_of_last_withdrawal = 0
        if last_withdrawal is not None:
            # convert created_at format date: datetime.datetime(2023, 4, 29, 9, 57, 36, 947000) to timemillis
            created_at_timemillis = int(time.mktime(
                last_withdrawal['created_at'].timetuple())) * 1000.0
            diff_days_of_last_withdrawal = (
                time.time() - created_at_timemillis) / (60 * 60 * 24)

        condition_status = (
            ta['status'] == 'active' or ta['status'] == 'pending_withdrawal')
        condition_withdrawal = (
            ta['withdrawal_days_traded'] == 15 or diff_days_of_last_withdrawal == 30)
        condition_remarks = 'k3pn' in ta['remarks'] or 'k2pa' in ta['remarks']
        if condition_status and condition_withdrawal and condition_remarks:
            return self.is_30seconds_rule(ta, trades)

        return False

    def is_30seconds_rule_for_check_target(self, ta, operation, current_remarks, trades):
        if 'k1pn' in current_remarks or 'k2pn' in current_remarks or 'k1pa' in current_remarks:
            if operation == 'win':
                return self.is_30seconds_rule(ta, trades)

        return False

    def get_cleaned_symbol(self, symbols, symbol):
        for s in symbols:
            if s['symbol'] == symbol and s['cleaned'] != '':
                return s['cleaned']
        return symbol

    def get_simple_cleaned_symbol(self, symbol):
        # for s in symbols:
        #     if s['symbol'] == symbol and s['cleaned'] != '':
        #         return s['cleaned']
        # return symbol

        if symbol == 'BTCUSDT' or symbol == 'BTCBUSD' or symbol == 'BTCUSD' or symbol == 'BTCEUR':
            symbol = 'BTC'
        elif symbol == 'ETHUSD' or symbol == 'ETHEUR':
            symbol = 'ETH'
        elif symbol == 'XRPUSD' or symbol == 'XRPEUR':
            symbol = 'XRP'

        return symbol

    def is_30seconds_rule(self, ta, trades):
        broker = ta['broker'] if 'broker' in ta else 'matchtrader'
        try:

            if self.is_30seconds_rule_supported(broker):
                remarks_obj = next((
                    item for item in self.remarksDB if item["key"] == ta['remarks'] and item['broker'] == ta['broker']), None)

                if remarks_obj is not None and '30_seconds_rule' in remarks_obj and remarks_obj['30_seconds_rule'] is True:
                    # logic for 30 seconds rule

                    total_trades = len(trades)

                    duration_milliseconds_minimum = 30 * 1000
                    print("total_trades: " + str(total_trades) +
                          " for mt_user_id " + str(ta['mt_user_id']))
                    if total_trades >= 20:
                        totals = {}
                        for trade in trades:
                            # print ("trade: " + str(trade))
                            instrument = trade['symbol']
                            instrument = self.get_simple_cleaned_symbol(
                                instrument)

                            # print ("instrument: " + str(instrument))

                            if instrument not in totals:
                                totals[instrument] = {
                                    'total_lots': 0,
                                    'trades': [],
                                    'total_trades': 0,
                                    'average': 0,
                                    'minimum_lots': 0,
                                }

                            # doesnt matter if its buy or sell, we need to get the absolute value
                            trade['lots'] = abs(trade['lots'])

                            if trade['lots'] > 0:
                                totals[instrument]['total_lots'] += trade['lots']
                                totals[instrument]['trades'].append(
                                    {'trader_id': trade['trader_id'], 'lots': trade['lots'], 'duration': trade['duration']})
                                totals[instrument]['total_trades'] = len(
                                    totals[instrument]['trades'])
                                totals[instrument]['average'] = totals[instrument]['total_lots'] / \
                                    len(totals[instrument]['trades'])
                                totals[instrument]['minimum_lots'] = totals[instrument]['average'] * 0.1

                        for instrument in totals:
                            print("instrument: " + str(instrument))
                            print("totals: " + str(totals[instrument]))

                        # filter trades in totals, that are above minimum_average
                        total_trades_accepted = 0
                        total_trades_less_30_seconds = 0
                        for instrument in totals:
                            totals[instrument]['trades_filtered'] = []
                            for trade in totals[instrument]['trades']:
                                if trade['lots'] > totals[instrument]['minimum_lots']:
                                    total_trades_accepted += 1
                                    # if duration < 30 seconds (30000 ms)
                                    if trade['duration'] < duration_milliseconds_minimum:
                                        total_trades_less_30_seconds += 1

                        if total_trades_accepted > 0:
                            percent_trades_filtered = total_trades_less_30_seconds / total_trades_accepted
                            print("total_trades_less_30_seconds: " + str(total_trades_less_30_seconds) +
                                  ", total_trades_accepted: " + str(total_trades_accepted) + " for mt_user_id " + str(ta['mt_user_id']))
                            # if total trades where duration > 30 seconds and profit > 10% of average, is lower than 95%
                            print("percent_trades_filtered less 30 seconds: " + str(
                                percent_trades_filtered * 100) + " for mt_user_id " + str(ta['mt_user_id']))
                            if percent_trades_filtered > 0.05:
                                print("is_30_seconds_rule for mt_user_id " +
                                      str(ta['mt_user_id']))
                                return True

        except Exception as e:
            print("Error getting trades: " + str(e))

            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            return False

        return False

    def detect_30seconds_rule_in_all_trading_accounts(self):
        # detect all trading accounts with hedging rule
        tas = self.trading_accounts.find_by_statuses(
            ['active', 'pending_withdrawal'])
        for ta in tas:
            self.detect_30seconds_rule_by_account(
                ta['mt_user_id'], ta['broker'])

    def detect_30seconds_rule_by_account(self, mt_user_id, broker="matchtrader"):
        mt_user_id = str(mt_user_id)
        # print ('detect_30seconds_rule_by_account: ' + mt_user_id)
        ta = self.trading_accounts.find_one_by_mt_user_id_and_broker(
            mt_user_id, broker)
        if ta is None:
            ta = self.trading_accounts.find_one_by_mt_user_id(mt_user_id)

        if ta is not None:
            # if self.is_30seconds_rule_when_withdrawal_allowed(ta):
            #     print('30 seconds rule withdrawal detected for user ' + str(ta['user_id']) + ' - ' + str(ta['mt_user_id']) + ' when withdrawal allowed')
            # else:
            #     print('30 seconds rule withdrawal NOT detected for user ' + str(ta['user_id']) + ' - ' + str(ta['mt_user_id']) + ' when withdrawal allowed')

            if self.is_30seconds_rule(ta):
                print('30 seconds rule detected for user ' +
                      str(ta['user_id']) + ' - ' + str(ta['mt_user_id']))
            else:
                print('30 seconds rule NOT detected for user ' +
                      str(ta['user_id']) + ' - ' + str(ta['mt_user_id']))

    def detect_hedging_rule_by_account(self, mt_user_id, broker='matchtrader'):
        mt_user_id = str(mt_user_id)
        ta = self.trading_accounts.find_one_by_mt_user_id_and_broker(
            mt_user_id, broker)
        if ta is None:
            ta = self.trading_accounts.find_one_by_mt_user_id(mt_user_id)
        if ta is not None:
            if self.is_hedging_rule(ta):
                print('Hedging rule detected for user ' +
                      str(ta['user_id']) + ' - ' + str(ta['mt_user_id']))
            else:
                print('Hedging rule NOT detected for user ' +
                      str(ta['user_id']) + ' - ' + str(ta['mt_user_id']))
        else:
            print('Trading account not found for mt_user_id ' + str(mt_user_id))

    def detect_hedging_rule_by_account_and_broker(self, mt_user_id, broker):
        return self.detect_hedging_rule_by_account(mt_user_id, broker)

    def close_trades_by_trading_account(self, ta):
        # update trades with closed status
        broker = ta['broker']
        self.trades.update_many({'mt_user_id': ta['mt_user_id'], 'broker': broker, 'status': 'active'}, {
                                'status': 'closed', 'updated_at': datetime.utcnow()})

    def close_current_account(self, user, ta, status_reason):

        if user is not None and ta is not None:
            utils.print_with_module("TaskManager.close_current_account", "Closing account for user " + str(
                user['User_email']), 'mt_user_id', ta['mt_user_id'])
            try:
                self.close_open_orders(ta)
                self.close_open_positions(ta)
                # wait 5 seconds
                if self.close_accounts:
                    time.sleep(5)
            except Exception as e:
                print("Error closing open positions: " + str(e))

            try:
                self.withdrawal_all_money(ta)
            except Exception as e:
                print("Error withdrawal all money: " + str(e))

            self.close_account(ta)

            self.close_local_trading_account(ta, status_reason)
            self.close_trades_by_trading_account(ta)

    def close_account_by_mt_user_id(self, mt_user_id, status_reason='Manual closing'):
        ta = self.trading_accounts.find_one_by_mt_user_id(str(mt_user_id))
        if ta is not None:
            user = self.users.find_one_by_id(ta['user_id'])
            self.close_current_account(user, ta, status_reason)

    def close_accounts_by_email(self, email, status_reason='Manual closing'):
        user = self.users.find_one_by_email(email)
        if user is not None:
            tas = self.trading_accounts.find_by_user_id(user['_id'])
            for ta in tas:
                self.close_current_account(user, ta, status_reason)

    def close_account(self, ta):
        # close account
        if self.close_accounts:
            broker = ta['broker'] if 'broker' in ta else 'matchtrader'
            self.broker_manager.close_account(ta['mt_user_id'], broker)
        else:
            broker = ta['broker'] if 'broker' in ta else 'matchtrader'
            print("close_account: close_accounts is False ",
                  ta['mt_user_id'], broker)

    def close_open_orders(self, ta):
        # close all open positions
        if self.close_accounts:
            broker = ta['broker'] if 'broker' in ta else 'matchtrader'
            self.broker_manager.close_open_orders(ta['mt_user_id'], broker)
        else:
            broker = ta['broker'] if 'broker' in ta else 'matchtrader'
            print("close_open_orders: close_accounts is False ",
                  ta['mt_user_id'], broker)

    def close_open_positions(self, ta):
        # close all open positions
        if self.close_accounts:
            broker = ta['broker'] if 'broker' in ta else 'matchtrader'
            self.broker_manager.close_open_positions(ta['mt_user_id'], broker)
        else:
            broker = ta['broker'] if 'broker' in ta else 'matchtrader'
            print("close_open_positions: close_accounts is False ",
                  ta['mt_user_id'], broker)

    def withdrawal_all_money(self, ta):
        # withdrawal all balance
        if self.close_accounts:
            broker = ta['broker'] if 'broker' in ta else 'matchtrader'
            self.broker_manager.withdrawal_all_money(ta['mt_user_id'], broker)
        else:
            broker = ta['broker'] if 'broker' in ta else 'matchtrader'
            print("withdrawal_all_money: close_accounts is False ",
                  ta['mt_user_id'], broker)

    def close_local_trading_account(self, ta, status_reason=''):
        elo = 0
        
        # 'hola que ase' 
        win_or_lost = status_reason.split(" ")[-1]
        
        if win_or_lost == 'win':
            elo = self.get_elo_for_phase_win(ta['remarks'])
        else:
            elo = self.get_elo_for_phase_lost(ta['remarks'])
                    
        if ta is not None:
            # close account, balance to 0, and closed_date
            data = {
                '$set': {
                    'balance': 0,
                    'closed_date': datetime.utcnow(),
                    'status': 'closed',
                    'status_reason': status_reason if status_reason is not None else '',
                    'closed_balance': ta['balance'],
                    'closed_equity': ta['equity'],
                    'updated_at': datetime.utcnow(),
                },
                '$inc': {
                    'elo': elo
                }
            }
            broker = ta['broker'] if 'broker' in ta else 'matchtrader'
            self.trading_accounts.db.collection.update_one({'broker': broker, 'mt_user_id': ta['mt_user_id']}, data)

    def get_next_remarks(self, current_remarks, broker='matchtrader'):
        # replace number k1pn to k2pn, and k2pn to k3pn
        new_remarks = str(current_remarks).lower()

        # if len(new_remarks) < 6:
        #     new_remarks = '10k1pn'

        utils.print_with_module(
            "TaskManager.get_next_remarks", "current_remarks: " + str(current_remarks))

        # 2 phases: k1pn, k2pn, k3pn
        if 'k1pn' in new_remarks:
            new_remarks = new_remarks.replace('k1pn', 'k2pn')
        elif 'k2pn' in new_remarks:
            new_remarks = new_remarks.replace('k2pn', 'k3pn')

        # 1 phase: k1pa, k2pa
        if 'k1pa' in new_remarks:
            new_remarks = new_remarks.replace('k1pa', 'k2pa')

        utils.print_with_module(
            "TaskManager.get_next_remarks", "new_remarks: " + str(new_remarks))

        obj = self.remarks.find_one_by_key_and_broker(new_remarks, broker)
        if obj is None:
            utils.print_with_module(
                "TaskManager.get_next_remarks", "remarks not found. returning default: 10k1pn")
            return '10k1pn'
        else:
            utils.print_with_module(
                "TaskManager.get_next_remarks", "remarks found: " + str(obj['key']))

        key = ''
        if obj and 'key' in obj:
            key = obj['key']

        return key

    def fix_remarks(self, remarks, sku):
        remarks = remarks.lower()
        sku = int(sku)
        # remarks_with_key_ending_in_fee = self.remarks.find({'key': {'$regex': 'fee$'}})
        # sku_fees = [r['sku'] for r in remarks_with_key_ending_in_fee]
        sku_fees = [6, 7, 8, 9, 10, 16, 17, 18, 19, 20]
        if sku in sku_fees:
            if not remarks.endswith('fee'):
                remarks = remarks + 'fee'

        # remarks_with_demo_start = self.remarks.find({'key': {'$regex': 'demo\\\\'}})
        # sku_demos = [r['sku'] for r in remarks_with_demo_start]
        sku_demos = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        if sku in sku_demos:
            if not remarks.startswith('demo\\'):
                remarks = 'demo\\' + remarks

        return remarks
    
    
        
    
        

    @utils.task_log('create_new_trading_account')
    def create_new_trading_account(self, user, ta):
        # create new trading account
        old_remarks = ta['remarks']
        print("create_new_trading_account: old_remarks: " + str(old_remarks))
        new_remarks = self.get_next_remarks(old_remarks, ta['broker'])
        if new_remarks != '':
            print("create_new_trading_account: new remarks: " + str(new_remarks))
            # remarks = self.remarks.find_one_by_key_and_broker(
            #     new_remarks, ta['broker'])

            remarks = next((r for r in self.remarks.find(
                {'key': new_remarks}) if r['broker'] == ta['broker']), None)

            print("create_new_trading_account: remarks: " + str(remarks))

            if remarks is None:
                utils.print_with_module("TaskManager.create_new_trading_account", "remarks " + str(
                    new_remarks) + " - broker " + str(ta['broker']) + " not found")

            offer_id = self.get_current_remarks_offer_id(remarks)
            print("create_new_trading_account: offer_id: " + str(offer_id))

            password = functions.generate_random_password()
            fullname = user['First_name'] + ' ' + user['last_name']
            fullname = fullname.strip()
            data = {
                'broker': ta['broker'],

                'fullname': fullname,
                'firstname': user['First_name'] if 'First_name' in user else '',
                'lastname': user['last_name'] if 'last_name' in user else '',

                'email': user['User_email'].strip(),
                'phone': user['phone'],
                'country': user['country'],
                'city': user['city'],
                'address': user['address'],
                'comment': 'New trading account',
                'created_at': datetime.utcnow(),
                'password': password,
                'origin_mt_user_id': ta['mt_user_id'],
                'offer_id': offer_id,
                'remarks': new_remarks,
                'balance': remarks['amount'] if 'amount' in remarks else 0.0,
                'start_balance': remarks['amount'] if 'amount' in remarks else 0.0,
            }

            utils.print_with_module("TaskManager.create_new_trading_account",
                                    "creating new trading account with data: " + str(data))

            broker = ta['broker'] if 'broker' in ta else 'matchtrader'
            if self.close_accounts:
                mt_user_id, creation_type = self.broker_manager.create_account(
                    data, broker)
                
                self.trading_accounts.db.collection.update_one({'mt_user_id': ta['mt_user_id']}, {"$set": {
                    "next_mt_user_id": mt_user_id}})
                print("mt_user_id: " + str(mt_user_id),
                      "creation_type: " + str(creation_type))
                if mt_user_id is not None:
                    # functions.update_broker_trading_account_remarks(mt_user_id, new_remarks)
                    self.broker_manager.update_remarks(
                        mt_user_id, new_remarks, broker)

                    if broker == 'metatrader':
                        # Notify n8n when a new user is created
                        # New credentials por metatrader account created (required for login in broker)
                        url = 'https://n8n.cr.cryptofundtrader.com/webhook/user-created-multibroker'
                        headers = {'Content-Type': 'application/json'}
                        email = utils.get_email_by_environment(
                            data['email']) if 'email' in data else ''

                        remarks_target = new_remarks
                        remarks_sku = remarks['sku'] if 'sku' in remarks else 0
                        data = {
                            'email': email,
                            'firstname': user['First_name'],
                            'login': user['Login_name'],  # dashboard login
                            'password': data['password'],  # meta password

                            'broker': broker,
                            'mt_user_id': mt_user_id,  # meta login
                            'creation_type': creation_type,

                            'remarks': remarks_target,
                            'sku': remarks_sku,

                            'app_url': utils.get_app_url_by_environment()
                        }
                        utils.do_post_request(url, headers, data)

            else:
                print("create_new_trading_account: close_accounts is False ")
        return old_remarks, new_remarks

    def get_current_remarks_offer_id(self, remarks):
        offer_id = remarks['offer_id_development']
        if os.getenv('ENV') == 'production':
            offer_id = remarks['offer_id_production']
        return offer_id

    @utils.task_log('update_positions_collection')
    def update_positions_collection(self):
        accounts = self.broker_manager.get_all_accounts(
            self.chunk_size,  self.chunk_number)

        # discard all accounts from metatrader broker
        # accounts = [a for a in accounts if a['broker'] == 'metatrader' and a['clientId'] == '123643']

        old_positions = self.positions.find_by_filter({})
        old_positions_ids = []
        for position in old_positions:
            old_positions_ids.append(position['trader_id'])

        positions = []
        accounts_by_broker = {}
        supported_brokers = self.broker_manager.get_supported_brokers()
        for broker in supported_brokers:
            accounts_by_broker[broker] = []

        for account in accounts:
            local_broker = account['broker']
            accounts_by_broker[local_broker].append(account['clientId'])

        for broker in accounts_by_broker:
            if len(accounts_by_broker[broker]) > 0:
                print("getting positions for broker: " + str(broker))
                print("length of accounts_by_broker: " +
                      str(len(accounts_by_broker[broker])))
                if self.broker_manager.is_hedging_rule_supported(broker) is True:
                    positions_broker = self.broker_manager.get_open_positions_by_mt_user_ids(
                        accounts_by_broker[broker], broker)

                    for position in positions_broker:
                        position['broker'] = broker
                        positions.append(position)

        # save all to database
        positions_added = 0
        for position in positions:
            broker = position['broker']
            id = position['trader_id']
            mt_user_id = str(position['clientId'])

            # check if position exists
            # print("checking if position exists: " + str(id) + " " + str(broker))
            if self.positions.exists_by_id_and_broker(id, broker) is False:
                ta = self.trading_accounts.find_one_by_mt_user_id_and_broker(
                    mt_user_id, broker)
                if ta is not None:
                    user_id = ta['user_id']
                    # create position
                    data = {
                        'trader_id': id,
                        'mt_user_id': mt_user_id,
                        'user_id': user_id,
                        'broker': broker,
                        'position': position,
                        'created_at': datetime.utcnow(),
                    }
                    positions_added += 1
                    self.positions.create(data)
            else:
                # If position exists in database, remove from old_positions_ids
                if id in old_positions_ids:
                    old_positions_ids.remove(id)

        if positions_added > 0:
            utils.print_with_module(
                "TaskManager.update_positions_collection", "positions added: " + str(positions_added))

        # remove old positions to discard old open positions
        if len(old_positions_ids) > 0:
            self.positions.remove_by_ids_and_broker(
                old_positions_ids, 'matchtrader')
            utils.print_with_module("TaskManager.update_positions_collection",
                                    "old positions removed: " + str(len(old_positions_ids)))

    @utils.task_log('check_30_seconds_rule_for_all_accounts')
    def check_30_seconds_rule_for_all_accounts(self):
        '''
            Es para revisar los trades que no tienen comision que no hagan operaciones en menos de 30 segundos, solo en las cuentas que no tienen comision

        '''
        try:
            cda.print_chunk_data(
                self, "CHECK_30_SECONDS_RULE_FOR_ALL_ACCOUNTS")

            users_db = cda.get_chunked_users(self)

            trading_accounts_db = cda.get_trading_accounts_by_users(users_db)

            last_withdrawals = cda.get_last_withdrawals_by_trading_accounts_set(
                self, trading_accounts_db)

            user_data = cda.users_to_set(users_db)

            self.remarksDB = cda.get_remarks_data()

            trades_by_user_data = cda.get_trades_actives_by_trading_accounts_set(
                trading_accounts_db)
            i = 0
            for ta in trading_accounts_db:
                # print("checking 30 seconds rule for i: " + str(i))
                i += 1
                last_withdrawal = last_withdrawals.get(ta['mt_user_id'], None)
                trades = trades_by_user_data.get(
                    ta['mt_user_id'] + '_' + ta['broker'], [])
                # print("ta: " + str(ta.get('mt_user_id')))
                # print("last_withdrawal: " + str(last_withdrawal))
                # print("trades: " + str(len(trades)))

                found = self.is_30seconds_rule_when_withdrawal_allowed(
                    ta, last_withdrawal, trades)
                # print("found: " + str(found))
                if (found):
                    user = user_data[str(ta['user_id'])]
                    if user is not None:
                        self.close_current_account(
                            user, ta, '30_seconds_rule_detected - withdrawal')
                        self.notify_30seconds_rule(ta)

        except Exception as e:
            print(e)
            print('Error in update_users_collection')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print(e)

    @utils.task_log('check_hedging_rule_for_all_accounts')
    def check_hedging_rule_for_all_accounts(self):
        try:
            # check if positions break hedging rule
            offending_accounts = self.detect_hedging_rule_in_all_accounts()

            if len(offending_accounts) > 0:
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
                users = list(User().db.collection.find({
                    '_id': {
                        '$in': [ObjectId(user['user_id']) for user in offending_accounts]
                    },
                    **filter
                }))
                users_set = cda.users_to_set(users)

                trading_accounts_db = cda.get_all_trading_accounts_by_offending_accounts(
                    offending_accounts)
                trading_accounts = cda.trading_account_to_set(
                    trading_accounts_db)
                for offending_account in offending_accounts:
                    print("offending account: " + str(offending_account))
                    user_id = str(offending_account['user_id'])
                    user = users_set.get(user_id, None)

                    closed_accounts = []
                    full_closed_accounts = []
                    if user is not None:
                        print("Recuperando cuenta: " +
                              str(offending_account['mt_user_id']))

                        mt_user_id = str(offending_account['mt_user_id'])
                        broker = offending_account['broker']
                        set_key = mt_user_id + '_' + broker
                        ta = trading_accounts.get(set_key, None)
                        if user is not None and ta is not None and ta['status'] == 'active':
                            closed_accounts.append(
                                offending_account['mt_user_id'])
                            full_closed_accounts.append(offending_account)
                            utils.send_telegram_message_with_title(self.telegrams_prefix + " Hedging rule detected", "User " + str(
                                user_id) + " and accounts: " + str(offending_account['mt_user_id']))
                            self.close_current_account(
                                user, ta, 'hedging_rule_detected')

                        # print('closed_accounts: ' + str(closed_accounts))
                        if len(closed_accounts) > 0:
                            url = "https://n8n.cryptofundtrader.com/webhook/rule-detected"
                            headers = {
                                'Content-Type': 'application/json'
                            }
                            data = {
                                'environment': os.getenv('ENVIRONMENT'),
                                'user_id': str(user_id),
                                'email': utils.get_email_by_environment(user['User_email']),
                                'firstname': user['First_name'],
                                'lastname': user['last_name'],
                                'mt_user_ids': closed_accounts,
                                'details': full_closed_accounts,
                                'rule': 'hedging',
                            }
                            if utils.is_n8n_webhook_enabled() and self.send_n8n_notifications:
                                print("sending webhook to n8n")
                                print(data)
                                response = utils.do_post_request(
                                    url, headers, data)
                                if response is not None and response.status_code == 200:
                                    utils.print_with_module("TaskManager.check_hedging_rule_for_all_accounts", "webhook for closing accounts for user " + str(
                                        user_id) + " and accounts: " + str(closed_accounts))

        except Exception as e:

            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print(e)

    @utils.task_log('update_users_collection')
    def update_users_collection(self, initial_batch=False):
        datemilliseconds = datetime.now()
        cda.print_chunk_data(self, "UPDATE_USERS_COLLECTION")

        try:

            bulk_users_created = []
            bulk_trading_accounts_created = []
            bulk_trading_accounts_updated = []

            users_created = []
            trading_accounts_created = []
            trading_accounts_updated = []
            trading_accounts_without_remarks = []

            broker_accounts = cda.get_broker_accounts_chunked(self)

            self.remarksDB = cda.get_remarks_data()

            # get first 10 accounts
            # accounts = accounts[:10]
            users = cda.get_users_by_broker_accounts_login_name_set(
                broker_accounts)
            trading_accounts = cda.get_all_trading_accounts_by_broker_accounts_set(
                broker_accounts)

            accounts_with_email = 0
            i = 0

            for account in broker_accounts:
                i += 1
                clientInfo = account['clientInfo']
                email = clientInfo['email']
                if email is not None and len(email) > 0:
                    accounts_with_email += 1
                    # user = self.users.find_one_by_email(email)
                    user = users.get(email, None)

                    if user is None:
                        # print(' cuenta no existe ' + str(i) + ' de ' + str(total))

                        # Generate dashboard account
                        firstname, lastname = functions.get_name_parts(
                            clientInfo['name'])
                        if firstname == 'firstname':
                            firstname = ''
                        if lastname == 'lastname':
                            lastname = ''

                        # create registration date using timeseconds since 1970
                        registration_date = datetime.fromtimestamp(
                            int(clientInfo['registrationDate']) / 1000.0)

                        # 12 characters by default
                        password = functions.generate_random_password()
                        if initial_batch == True:
                            # hashed = hashlib.sha256("1234567890".encode("utf-8")).hexdigest()
                            hashed = functions.get_hashed_password(
                                "1234567890")
                        else:
                            # hashed = hashlib.sha256(password.encode("utf-8")).hexdigest()
                            hashed = functions.get_hashed_password(password)

                        is_admin = False
                        if email == 'accounts@cryptofundtrader.com':
                            is_admin = True

                        new_user = {
                            'Login_name': email,
                            'User_pass_hashed': hashed,
                            'User_email': email,
                            'First_name': firstname,
                            'last_name': lastname,
                            'address': clientInfo['address'],
                            'city': clientInfo['city'],
                            'zipcode': clientInfo['zipCode'],
                            'province': clientInfo['province'],
                            'country': clientInfo['country'],
                            'phone': clientInfo['phone'],
                            'date_registered': registration_date,
                            'admin': is_admin,
                            'elo': 0,
                            'source': 'user-created-on-sync',
                            'created_at': datetime.utcnow(),
                        }
                        bulk_users_created.append(InsertOne(new_user))

                        users_created.append({
                            'email': email,
                            'firstname': firstname,
                            'login': clientInfo['login'],
                            'password': password
                        })

                    # Get current user trading account
                    if user is not None:
                        mt_user_id = str(account['clientId'])
                        broker = account['broker']
                        set_key = mt_user_id + '_' + broker

                        trading_account = trading_accounts.get(set_key, None)

                        remarks = clientInfo['remarks'].lower()
                        amount = self.get_amount_from_remarks(remarks)
                        if trading_account is None:

                            if remarks == '':
                                trading_accounts_without_remarks.append(
                                    clientInfo['clientId'] + '-' + user['User_email'])
                            else:
                                ta_limits = self.get_account_limits_by_broker(
                                    True, remarks, utils.adapt_integer_amount_to_decimal_amount(float(account['balance'])), account['broker'])

                                status = 'active'
                                if clientInfo['isLocked']:
                                    status = 'closed'

                                max_balance = utils.adapt_integer_amount_to_decimal_amount(
                                    int(account['balance']))
                                trailing_drawdown = max_balance - \
                                    (max_balance * 0.06)

                                ta_create_data = {
                                    'mt_user_id': account['clientId'],
                                    'user_id': user['_id'],
                                    'broker': account['broker'],
                                    'remarks': remarks,
                                    'balance': utils.adapt_integer_amount_to_decimal_amount(int(account['balance'])),
                                    'max_balance': max_balance,
                                    'min_balance': max_balance,
                                    'equity': utils.adapt_integer_amount_to_decimal_amount(int(account['equity'])),
                                    'days_traded': 0,
                                    'start_balance': amount,
                                    'daily_drawdown': ta_limits['daily_drawdown'],
                                    'max_drawdown': ta_limits['max_drawdown'],
                                    'trailing_drawdown': trailing_drawdown,
                                    'profit_target': ta_limits['target_profit'],
                                    'avg_return_trade': 0,
                                    'win_rate': 0,
                                    'elo': 0,
                                    'total_trades': 0,
                                    'withdrawal_days_traded': 0,
                                    'withdrawal_last_request': None,
                                    'status': status,
                                    'source': 'sync-task-update_users',
                                    'created_at': datetime.utcnow(),
                                }
                                bulk_trading_accounts_created.append(
                                    InsertOne(ta_create_data))

                                # add tradding account
                                # TODO START
                                # self.trading_accounts.create(ta)
                                trading_accounts_created.append(
                                    clientInfo['clientId'] + '-' + user['User_email'])
                                utils.print_with_module(
                                    "TaskManager.update_users_collection", "trading_account created for user " + user['User_email'])
                                # TODO END

                        else:
                            balance = utils.adapt_integer_amount_to_decimal_amount(
                                int(account['balance']))
                            equity = utils.adapt_integer_amount_to_decimal_amount(
                                int(account['equity']))
                            if (balance != trading_account['balance'] or equity != trading_account['equity']) and remarks != '':
                                # Update balance + equity
                                filter = {
                                    'mt_user_id': str(clientInfo['clientId']),
                                    'user_id': user['_id'],
                                    'broker': account['broker']
                                }

                                max_balance = trading_account[
                                    'max_balance'] if 'max_balance' in trading_account else trading_account['balance']
                                min_balance = trading_account[
                                    'min_balance'] if 'min_balance' in trading_account else trading_account['balance']

                                if balance > max_balance:
                                    max_balance = balance
                                if balance < min_balance:
                                    min_balance = balance

                                trailing_drawdown = max_balance - \
                                    (max_balance * 0.06)

                                remarks_amount = self.get_amount_from_remarks(
                                    remarks)
                                diff = max_balance - remarks_amount
                                # diff is greater than 6%
                                if diff > (remarks_amount * 0.06):
                                    trailing_drawdown = remarks_amount

                                ta_update_data = {
                                    'remarks': remarks,
                                    'balance': balance,
                                    'max_balance': max_balance,
                                    'min_balance': min_balance,
                                    'trailing_drawdown': trailing_drawdown,
                                    'equity': equity,
                                    'source': 'sync-task-update_users',
                                    'updated_at': datetime.utcnow(),
                                }

                                bulk_trading_accounts_updated.append(
                                    UpdateOne(filter, {'$set': ta_update_data}))
                                # TODO START
                                # self.trading_accounts.update_one(filter, data)
                                # utils.print_with_module("TaskManager.update_users_collection", "trading_account updated for user " + user['User_email'] + " balance: " + str(balance) + " equity: " + str(equity))
                                trading_accounts_updated.append(
                                    account['clientId'] + "-" + user['User_email'])
                                # TODO END

            try:
                if len(bulk_users_created) > 0:
                    self.users.db.collection.bulk_write(
                        bulk_users_created, ordered=False)

                if len(bulk_trading_accounts_created) > 0 or len(bulk_trading_accounts_updated) > 0:
                    self.trading_accounts.db.collection.bulk_write(
                        bulk_trading_accounts_created + bulk_trading_accounts_updated, ordered=False)
            except BulkWriteError as e:
                for error in e.errors:
                    print(error.index, error.code, error.message)
                print(bulk_users_created)
                print("Error al crear usuarios")

            # print ("accounts with email: " + str(accounts_with_email))
            if len(users_created) > 0:
                utils.print_with_module(
                    "TaskManager.update_users_collection", "users created: " + str(len(users_created)))
            if len(trading_accounts_created) > 0:
                utils.print_with_module("TaskManager.update_users_collection",
                                        "trading_accounts created: " + str(len(trading_accounts_created)))
            if len(trading_accounts_updated) > 0:
                utils.print_with_module("TaskManager.update_users_collection",
                                        "trading_accounts updated: " + str(len(trading_accounts_updated)))
            if len(trading_accounts_without_remarks) > 0:
                utils.print_with_module("TaskManager.update_users_collection",
                                        "trading_accounts without remarks: " + str(len(trading_accounts_without_remarks)))
            print("Procesa la cola " + str(datetime.now() - datemilliseconds))

            return users_created

        except Exception as e:
            print('Error in update_users_collection')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print(e)

    def get_account_limits_by_broker(self, new, remarks='10k1pn', balance=0, broker='matchtrader'):
        daily_drawdown = 0
        max_drawdown = 0
        target_profit = 0
        offer_id = ''

        if balance is None:
            balance = 0
        elif balance > 0:
            balance = float(balance)

        amount = 0
        # remark = self.remarks.find_one_by_key(remarks)
        # remark = self.remarks.find_one_by_key_and_broker(remarks, broker)
        remark = next(
            (rm for rm in self.remarksDB if rm['key'] ==
             remarks and rm['broker'] == broker), None
        )

        if remark is not None:
            target_profit = remark['target_profit']
            max_drawdown = remark['max_drawdown']
            daily_drawdown = remark['daily_drawdown'] if new else balance * 0.95
            offer_id = remark['offer_id_development']
            if os.environ['ENVIRONMENT'] == 'production':
                offer_id = remark['offer_id_production']

        obj = {
            'amount': amount,
            'daily_drawdown': daily_drawdown,
            'max_drawdown': max_drawdown,
            'target_profit': target_profit,
            'offer_id': offer_id
        }

        utils.print_with_module(
            "TaskManager.get_account_limits_by_broker", "Account limits: " + str(obj))

        return obj

    def get_account_limits(self, new, remarks='10k1pn', balance=0):
        daily_drawdown = 0
        max_drawdown = 0
        target_profit = 0
        offer_id = ''

        if balance is None:
            balance = 0
        elif balance > 0:
            balance = float(balance)

        amount = 0
        remark = self.remarks.find_one_by_key(remarks)
        if remark is not None:
            target_profit = remark['target_profit']
            max_drawdown = remark['max_drawdown']
            daily_drawdown = remark['daily_drawdown'] if new else balance * 0.95
            offer_id = remark['offer_id_development']
            if os.environ['ENVIRONMENT'] == 'production':
                offer_id = remark['offer_id_production']

        obj = {
            'amount': amount,
            'daily_drawdown': daily_drawdown,
            'max_drawdown': max_drawdown,
            'target_profit': target_profit,
            'offer_id': offer_id
        }

        utils.print_with_module(
            "TaskManager.get_account_limits", "Account limits: " + str(obj))

        return obj

    def get_amount_from_remarks(self, remarks):
        amount = 0
        remark = next(
            (rm for rm in self.remarksDB if rm['key'] == remarks), None
        )
        if remark is not None:
            amount = remark['amount']

        # print(amount)

        return amount

    @utils.task_log('check_targets_and_update')
    def check_targets_and_update(self):
        try:
            datemilliseconds = datetime.now()
            cda.print_chunk_data(self, "UPDATE_TRADING_ACCOUNTS_BALANCES")

            print('chunk_size: ' + str(self.chunk_size))
            print('chunk_number: ' + str(self.chunk_number))

            broker_accounts = cda.get_broker_accounts_chunked(
                self)  # filtrar las locked

            self.remarksDB = cda.get_remarks_data()

            users = cda.get_users_by_broker_accounts_login_name_set(
                broker_accounts)

            trading_accounts_db = cda.get_trading_accounts_by_broker_accounts_active_pending_withdrawal(
                broker_accounts)

            trading_accounts = cda.trading_account_to_set(trading_accounts_db)

            trades_by_user_data = cda.get_trades_actives_by_trading_accounts_set(
                trading_accounts_db)

            # i = 0

            for account in broker_accounts:
                # print(' account: ' + str(i) + ' de ' +
                #       str(len(broker_accounts)))
                # i += 1
                clientInfo = account['clientInfo']
                email = clientInfo['email']
                mt_user_id = str(account['clientId'])
                broker = account['broker']
                set_key = mt_user_id + '_' + broker
                # print(account['clientInfo']['email'])
                trades = trades_by_user_data.get(set_key, [])

                trading_account = trading_accounts.get(set_key, None)

                user = users.get(email, None)

                self.check_target_and_update(
                    account, user, trading_account, trades)
            print("Procesa la cola de check_targets_and_update " +
                  str(datetime.now() - datemilliseconds))

        except Exception as e:
            print('Error in update_users_collection')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print(e)

    def check_30seconds_rule(self, account):
        ta = self.trading_accounts.find_one_by_mt_user_id_and_broker(
            account['clientId'], account['broker'])
        if ta is not None and 'status' in ta:
            if 'remarks' in ta and ta['remarks'] != '':
                remarks_obj = self.remarks.find_one_by_key_and_broker(
                    ta['remarks'], ta['broker'])
                if remarks_obj is not None and '30_seconds_rule' in remarks_obj and remarks_obj['30_seconds_rule'] == True:
                    if self.is_30seconds_rule(ta):
                        self.detect_and_notify_30seconds_rule(ta)
        return False

    def check_target_and_update_by_account(self, mt_user_id):
        mt_user_id = str(mt_user_id)
        ta = self.trading_accounts.find_one_by_mt_user_id(mt_user_id)
        if ta is not None:
            accounts = self.broker_manager.get_all_accounts(
                self.chunk_size,  self.chunk_number)
            # filter by mt_user_id
            for account in accounts:
                if account['clientId'] == mt_user_id:
                    print(ta)
                    print(account)
                    self.check_target_and_update(account)
                    # Reset status
                    self.trading_accounts.update_one_by_mt_user_id_and_broker(
                        mt_user_id, account['broker'], {'status': 'active'})
                    break

    def create_new_trading_account_by_mt_user_id(self, mt_user_id, broker):
        mt_user_id = str(mt_user_id)
        ta = self.trading_accounts.find_one_by_mt_user_id(mt_user_id)
        if ta is not None:
            user = self.users.find_one_by_id(ta['user_id'])
            old_remarks, new_remarks = self.create_new_trading_account(
                user, ta)
            print("WIN: create new trading_account")
            print("WIN: old_remarks " + str(old_remarks))
            print("WIN: new_remarks " + str(new_remarks))

    def print_with_basic_data(self, ta, message):
        print("Ta " + str(ta['mt_user_id']) + ", " +
              str(ta['broker']) + ": " + message)

    def check_target_and_update(self, account, user, ta, trades):
        try:
            balance = utils.adapt_integer_amount_to_decimal_amount(
                int(account['balance']))
            equity = utils.adapt_integer_amount_to_decimal_amount(
                int(account['equity']))
            # ta = self.trading_accounts.find_one_by_mt_user_id_and_broker(account['clientId'], account['broker'])
            if ta is not None and 'status' in ta:
                # este if sobra porque ya se filtra por remarks != ''
                if ta['status'] == 'active' and 'remarks' in ta and ta['remarks'] != '':
                    # remarks is equal to preliminary, discard it
                    if ta['remarks'] == 'preliminary':
                        utils.print_with_module("TaskManager.check_target_and_update", "Discarding account " + str(
                            account['clientId']) + " because remarks is preliminary")
                    else:
                        # user = self.users.find_one_by_id(ta['user_id'])
                        profit_target = ta['profit_target'] if 'profit_target' in ta else 0
                        current_remarks = next(
                            (rm for rm in self.remarksDB if rm['key'] ==
                             ta['remarks'] and rm['broker'] == ta['broker']), None
                        )

                        # current_remarks = next(
                        #     (rm for rm in self.remarksDB if rm['key'] == ta['remarks'] and rm['broker'] == ta['broker']),
                        # )
                        operation = ''
                        status_reason = ''

                        tournament_remarks = list(self.remarks.db.collection.find({'key': {'$in': [
                            'demo\\duje_matic',
                            'demo\\cryptorisegroup',
                            'demo\\ustatrader',
                            'demo\\thissdax',
                            'demo\\traderomercan',
                            'demo\\traderoasis',
                            'demo\\marresecira',
                            'demo\\luya_madiba',
                            'demo\\trader_smaug',
                            'demo\\lightofkingdom',
                            'demo\\failedtosucceed',
                            'demo\\mrraja1326',
                            'demo\\mona_trades'
                        ]}}, {'key': 1, 'amount': 1}))

                        tournament_remarks_keys = [r['key']
                                                   for r in tournament_remarks]

                        if 'k1pn' in ta['remarks'] or 'k2pn' in ta['remarks'] or 'k3pn' in ta['remarks']:
                            if balance >= profit_target and ta['days_traded'] >= 5 and 'k3pn' not in ta['remarks']:
                                operation = 'win'
                            elif equity <= ta['daily_drawdown'] or equity <= ta['max_drawdown']:
                                operation = 'loss'
                        elif 'k1pa' in ta['remarks'] or 'k2pa' in ta['remarks']:
                            if balance >= profit_target and ta['days_traded'] >= 5 and 'k2pa' not in ta['remarks']:
                                operation = 'win'
                            elif equity <= ta['daily_drawdown'] or equity <= ta['trailing_drawdown']:
                                operation = 'loss'
                        elif ta['remarks'] in tournament_remarks_keys:
                            if equity <= ta['daily_drawdown'] or equity <= ta['max_drawdown']:
                                operation = 'loss'
                       
                
                        
                        if operation == 'loss':
                            elo_value = self.get_elo_for_phase_lost(ta['remarks'])
                            
                            update = {
                                        "$inc": {
                                            "elo": elo_value
                                        }, 
                                        "$set": {
                                            'elo_updated_at': datetime.now()
                                        }
                                    }
                            self.users.db.collection.update_one({'_id': user['_id']}, update)
                        
                        if operation == 'win':
                            elo_value = self.get_elo_for_phase_win(ta['remarks'])
                            
                            update = {
                                        "$inc": {
                                            "elo": elo_value
                                        }, 
                                        "$set": {
                                            'elo_updated_at': datetime.now()
                                        }
                                    }
                        
                            self.users.db.collection.update_one({'_id': user['_id']}, update)
                        
                        
                        
                        if operation != '':
                            self.print_with_basic_data(
                                ta, "Detected operation: " + str(operation))
                            if operation == 'win':
                                if 'k1pn' in ta['remarks'] or 'k2pn' in ta['remarks']:
                                    self.print_with_basic_data(ta, "win - balance: " + str(balance) + " - profit_target: " + str(
                                        profit_target) + " - days_traded: " + str(ta['days_traded']) + " - remarks: " + str(ta['remarks']))
                                else:
                                    # print ("Detection for mt_user_id " + str(ta['mt_user_id']) + " - " + str(ta['broker']) + ") (win) - equity: " + str(equity) + " - daily_drawdown: " + str(ta['daily_drawdown']) + " - trailing_drawdown: " + str(ta['trailing_drawdown']) + " - remarks: " + str(ta['remarks']))
                                    self.print_with_basic_data(ta, "win - equity: " + str(equity) + " - daily_drawdown: " + str(
                                        ta['daily_drawdown']) + " - trailing_drawdown: " + str(ta['trailing_drawdown']) + " - remarks: " + str(ta['remarks']))
                            else:
                                if 'k1pn' in ta['remarks'] or 'k2pn' in ta['remarks']:
                                    # print ("Detection for mt_user_id " + str(ta['mt_user_id']) + " - " + str(ta['broker']) + ") (loss) - equity: " + str(equity) + " - daily_drawdown: " + str(ta['daily_drawdown']) + " - max_drawdown: " + str(ta['max_drawdown']) + " - remarks: " + str(ta['remarks']))
                                    self.print_with_basic_data(ta, "loss - equity: " + str(equity) + " - daily_drawdown: " + str(
                                        ta['daily_drawdown']) + " - max_drawdown: " + str(ta['max_drawdown']) + " - remarks: " + str(ta['remarks']))
                                elif ta['remarks'] in tournament_remarks_keys:
                                    # print ("Detection for mt_user_id " + str(ta['mt_user_id']) + " - " + str(ta['broker']) + ") (loss) - equity: " + str(equity) + " - daily_drawdown: " + str(ta['daily_drawdown']) + " - max_drawdown: " + str(ta['max_drawdown']) + " - remarks: " + str(ta['remarks']))
                                    self.print_with_basic_data(ta, "loss - equity: " + str(equity) + " - daily_drawdown: " + str(
                                        ta['daily_drawdown']) + " - max_drawdown: " + str(ta['max_drawdown']) + " - remarks: " + str(ta['remarks']))
                                else:
                                    # print ("Detection for mt_user_id " + str(ta['mt_user_id']) + " - " + str(ta['broker']) + ") (loss) - equity: " + str(equity) + " - daily_drawdown: " + str(ta['daily_drawdown']) + " - trailing_drawdown: " + str(ta['trailing_drawdown']) + " - remarks: " + str(ta['remarks']))
                                    self.print_with_basic_data(ta, "loss - equity: " + str(equity) + " - daily_drawdown: " + str(
                                        ta['daily_drawdown']) + " - trailing_drawdown: " + str(ta['trailing_drawdown']) + " - remarks: " + str(ta['remarks']))

                        # if k3pn, profit_target is always empty = dont launch win operation
                        if operation == 'win' and ('k3pn' in ta['remarks'] or 'k2pa' in ta['remarks'] or ta['remarks'] in tournament_remarks_keys):
                            operation = ''

                        executed_30seconds_rule = False
                        if operation == 'win':
                            # check 30seconds rule
                            print("check 30seconds rule")
                            print(current_remarks)
                            if current_remarks and '30_seconds_rule' in current_remarks and current_remarks['30_seconds_rule'] == True:
                                if self.is_30seconds_rule_for_check_target(ta, operation, current_remarks['key'], trades):
                                    print("30seconds rule detected for mt_user_id: " +
                                          str(ta['mt_user_id']) + " and broker: " + str(ta['broker']))
                                    utils.send_telegram_message_with_title(self.telegrams_prefix + " 30 seconds rule detected", "User mt_user_id: " + str(
                                        ta['mt_user_id']) + " and broker: " + str(ta['broker']) + " in check_target_and_update")

                                    self.close_current_account(
                                        user, ta, '30seconds_rule_detected')

                                    self.notify_30seconds_rule(ta)
                                    executed_30seconds_rule = True
                                else:
                                    print("30seconds rule NOT detected for mt_user_id: " +
                                          str(ta['mt_user_id']) + " and broker: " + str(ta['broker']))

                        # print current remarks, operation, balance, profit_target
                        # utils.print_with_module("TaskManager.check_target_and_update", "checking trading_account: " + str(ta['mt_user_id']) + ". remarks: " + str(ta['remarks']) + ". balance: " + str(balance) + ". equity: " + str(equity) + ". daily_drawdown: " + str(ta['daily_drawdown']) + ". profit: " + str(ta['profit_target']) + ". max_drawdown: " + str(ta['max_drawdown']) + ". operation: " + str(operation) + ". executed_30seconds_rule: " + str(executed_30seconds_rule))

                        # FORCED: REMOVE
                        # operation = 'win'

                        # If no rule breached, check if win or loss
                        if executed_30seconds_rule == False:
                            # print mt_user_id, remarks, balance, profit_target, operation
                            if operation == 'win' or operation == 'loss':
                                certificate = False
                                certificate_url = ''

                                # create account with next remarks
                                old_remarks = ta['remarks']
                                new_remarks = ''

                                if operation == 'win' or operation == 'loss':
                                    utils.print_with_module("TaskManager.check_target_and_update", "checking trading_account: " + str(ta['mt_user_id']) + ". remarks: " + str(ta['remarks']) + ". balance: " + str(balance) + ". equity: " + str(
                                        equity) + ". daily_drawdown: " + str(ta['daily_drawdown']) + ". profit: " + str(ta['profit_target']) + ". max_drawdown: " + str(ta['max_drawdown']) + ". days_trade: " + str(ta['days_traded']) + ". operation: " + str(operation))

                                # if win or loss, close current account
                                self.close_current_account(
                                    user, ta, status_reason or 'closed by win/loss: ' + operation)

                                if operation == 'win':
                                    # broker: create new trading_account
                                    old_remarks, new_remarks = self.create_new_trading_account(
                                        user, ta)
                                    print("WIN: create new trading_account")
                                    print("WIN: old_remarks " +
                                          str(old_remarks))
                                    print("WIN: new_remarks " +
                                          str(new_remarks))
                                    if 'k3pn' in new_remarks or 'k2pa' in new_remarks:
                                        certificate = True
                                        if self.wordpress_task:

                                            current_dir = os.path.dirname(
                                                os.path.realpath(__file__))
                                            downloads_dir = os.path.join(
                                                current_dir, '../../downloads')
                                            result_upload = functions.upload_wordpress_certificate(downloads_dir,
                                                                                                   user, ta)
                                            utils.print_with_module(
                                                "TaskManager.check_target_and_update", "certificate uploaded to wordpress: " + str(result_upload))
                                            if result_upload is not None:
                                                if 'media' in result_upload:
                                                    if 'url' in result_upload['media']:
                                                        certificate_url = result_upload['media']['url']

                                # append new user to targets_reached
                                data = {
                                    'environment': os.getenv('ENVIRONMENT'),

                                    'user_id': str(ta['user_id']),
                                    'broker': ta['broker'],
                                    'mt_user_id': ta['mt_user_id'],

                                    'email': utils.get_email_by_environment(user['User_email']),
                                    'firstname': user['First_name'],
                                    'lastname': user['last_name'],


                                    'operation': operation,
                                    'status_reason': status_reason,

                                    'old_remarks': old_remarks,
                                    'new_remarks': new_remarks,
                                    # format to 2 decimals
                                    'daily_drawdown': '${:.2f}'.format(ta['daily_drawdown']),
                                    'max_drawdown': '${:.2f}'.format(ta['max_drawdown']),
                                    'equity': '${:.2f}'.format(equity),

                                    'certificate': certificate,
                                    'certificate_url': certificate_url
                                }
                                targets_reached = []
                                targets_reached.append(data)

                                if utils.is_n8n_webhook_enabled() and self.send_n8n_notifications:
                                    # call n8n webhook to notify admin
                                    url = 'https://n8n.cr.cryptofundtrader.com/webhook/targets-reached-new-remarks'
                                    headers = {
                                        'Content-Type': 'application/json'
                                    }
                                    data = {
                                        'users': targets_reached,
                                        'app_url': utils.get_app_url_by_environment()
                                    }
                                    response = utils.do_post_request(
                                        url, headers, data)
                                    if response.status_code != 200:
                                        utils.print_with_module(
                                            "TaskManager.check_target_and_update", "targets reached webhook error " + str(response.status_code))
                                    else:
                                        utils.print_with_module(
                                            "TaskManager.check_target_and_update", "targets reached webhook called")

        except Exception as e:
            print('Error in update_users_collection')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print(e)

    def upload_wordpress_certificate_test(self):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        downloads_dir = os.path.join(current_dir, '../../downloads')
        result_upload = functions.upload_wordpress_certificate(downloads_dir,
                                                               {'last_name': 'test', 'First_name': 'Andres', '_id': 'prueba'}, {})
        print(result_upload)

    @utils.task_log('update_trades')
    def update_trades(self):
        # pendiente de optimizar queries internamente
        self.update_trades_collection()
        self.update_trading_accounts_statistics()

    def get_last_24_hours_trades_by_trading_account(self, trading_account):
        # get last 24 hours trades
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        yesterday = yesterday.strftime('%Y-%m-%d %H:%M:%S')
        trades = self.trades.find(
            {'mt_user_id': trading_account['mt_user_id'], 'created_at': {'$gte': yesterday}})

        return trades
    
    def get_elo_for_win_trade(self, remarks):
        if 'k1pn' in remarks:
            return 3
        elif 'k2pn' in remarks:
            return 5
        elif 'k3pn' in remarks:
            return 8
        elif 'k1pa' in remarks:
            return 4
        elif 'k2pa' in remarks:
            return 7
        else:
            return 0
        
    def get_elo_for_lost_trade(self, remarks):
        if 'k1pn' in remarks:
            return 1.5
        elif 'k2pn' in remarks:
            return 2.5
        elif 'k3pn' in remarks:
            return 4
        elif 'k1pa' in remarks:
            return 2
        elif 'k2pa' in remarks:
            return 3.5 
        else:
            return 0 
            
    def get_elo_for_phase_win(self,remarks):
        if 'k1pn' in remarks:
            return 30
        if 'k2pn' in remarks:
            return 50
        if 'k1pa' in remarks:
            return 80
        else:
            return 0
    
    def get_elo_for_phase_lost(self, remarks):
        if 'k1pn' in remarks:
            return -20
        if 'k2pn' in remarks:
            return -40
        if 'k3pn' in remarks:
            return -80
        if 'k1pa' in remarks:
            return -30
        if 'k2pa' in remarks:
            return -100
        else:
            return 0
        
                
    def is_valid_object(self, object_id):
        return ObjectId.is_valid(object_id)       
 
    def get_trade_to_insert(self, ledger, trade, trading_account_data):
        added_trade = False
        # trade = self.trades.find_one_by_id_and_broker(
        #     ledger['uid'], ledger['broker'])
        if trade is None:
            # duration in seconds between open and close
            created_at = datetime.fromtimestamp(
                int(ledger['generatedTime']) / 1000.0)
            closed_open_time = datetime.fromtimestamp(
                int(ledger['closedOpenTime']) / 1000.0)
            amount = int(ledger['amount'])

            # get direction
            ledger['closedVolume'] = int(ledger['closedVolume'])
            direction = 'sell' if ledger['closedVolume'] < 0 else 'buy'

            duration = int(ledger['generatedTime']) - \
                int(ledger['closedOpenTime'])
            # assign open_time using closedOpenTime with format MM/dd/yyyy
            open_time = closed_open_time.strftime('%m/%d/%Y')
            close_time = created_at.strftime('%m/%d/%Y')
            closed_volume = utils.adapt_integer_amount_to_decimal_amount(
                int(ledger['closedVolume']))
            swap = utils.adapt_integer_amount_to_decimal_amount(
                int(ledger['closedSwap']))
            commission = utils.adapt_integer_amount_to_decimal_amount(
                int(ledger['closedCommission']))
            profit = utils.adapt_integer_amount_to_decimal_amount(int(amount))
            
            elo_percent = profit *100 / trading_account_data['balance'] if trading_account_data['balance'] > 0 else 0
            
            remarks = trading_account_data['remarks']
            
            if elo_percent > 0:
                elo_value = elo_percent / 0.1 * self.get_elo_for_win_trade(remarks)
            elif elo_percent < 0:
                elo_value = elo_percent / 0.1 * self.get_elo_for_lost_trade(remarks)
            else:
                elo_value = 0
            
            data = {
                'trader_id': ledger['uid'],
                'broker': ledger['broker'],
                'broker_trade_id': ledger['tradeId'],
                'mt_user_id': str(ledger['clientId']),
                'user_id': trading_account_data['user_id'],
                'current_balance': trading_account_data['balance'],
                
                'elo_percent': elo_percent,
                
                'elo_value': elo_value,
                
                'direction': direction,
                'lots': closed_volume,
                'open_time': open_time,
                'open_time_original': int(ledger['closedOpenTime']),
                'close_time': close_time,
                'close_time_original': int(ledger['generatedTime']),
                'duration': duration,
                'symbol': ledger['instrument'],
                'swap': swap,
                'commission': commission,
                'profit': profit,
                'comment': ledger['comment'],
                'status': 'active',
                'created_at': created_at
            }
            # self.trades.create(data)
            # print ("trade added for user " + ledger['clientId'])
            return data

        return added_trade

    
    @utils.task_log('update_trades_collection')
    def update_trades_collection(self, initial_batch=False):
        print('empieza a pedir closed_trades')

        closed_trades_broker = self.broker_manager.get_closed_trades(
            self.chunk_size,  self.chunk_number)
        # filtramos por fecha #
        fecha_fin = datetime.utcnow()
        fecha_inicio = fecha_fin - timedelta(days=2)
        closed_trades = [
            trade for trade in closed_trades_broker
            if fecha_inicio <= datetime.fromtimestamp(
                int(trade['generatedTime']) / 1000.0) <= fecha_fin
        ]
        print('acaba de pedir closed_trades')
        utils.print_with_module("TaskManager.update_trades_collection",
                                "Closed trades found: " + str(len(closed_trades)))

        trades_added = 0
        trades_to_create = []
        trades_uids = [ledger['uid'] for ledger in closed_trades]
        trades_uids_uniques = list(set(trades_uids))
        print('trades_uids_uniques', len(trades_uids_uniques))

        trades = cda.get_trades_by_trader_ids_set(trades_uids_uniques)

        mt_user_ids_unique = list(set([str(ledger['clientId']) for ledger in closed_trades]))
        
        trading_accounts_data = cda.get_trading_accounts_by_trades_only_mt_user_id_and_balance(mt_user_ids_unique )
        
        # tenemos que pedir los usuarios de cft
        cft_users = self.users.db.collection.find(
            {'Login_name': {'$regex': 'cryptofundtrader.com'}}, {'_id': 1})

        ## nos hace falta tener todos los usuarios y trading accounts de los trades.
        
        # aqui tienes todos los mt_users_ids, aparte del user_id y balance

        mt_user_id_elo = {}
        user_id_elo = {}
        
        for ledger in closed_trades:
            trading_account_data = trading_accounts_data.get(str(ledger['clientId']), None)
            
            if trading_account_data:
                user_id = trading_account_data['user_id']
                mt_user_id = trading_account_data['mt_user_id']
                
                if str(user_id) in [str(u['_id']) for u in cft_users]:
                    if os.getenv('TEST_ENVIRONMENT', 'False') != 'True':
                        continue
                else:
                    if os.getenv('TEST_ENVIRONMENT', 'False') == 'True':
                        continue
                trade = trades.get(ledger['uid'], None)
                added_trade = self.get_trade_to_insert(ledger, trade, trading_account_data)
                if added_trade:
                    trades_to_create.append(added_trade)
                    trades_added += 1
             
                    
                    if mt_user_id not in mt_user_id_elo:
                        mt_user_id_elo[mt_user_id] = {'elo_value': 0, 'mt_user_id': mt_user_id}
                    else:
                        mt_user_id_elo[mt_user_id]['elo_value'] += added_trade['elo_value']

                        
                        
                    
                    
                    if str(user_id) not in user_id_elo:
                        user_id_elo[str(user_id)] = {'elo_value': 0, 'user_id': user_id}
                    else:
                        user_id_elo[str(user_id)]['elo_value'] += mt_user_id_elo[mt_user_id]['elo_value']
        

            
        ta_to_update = [
            UpdateOne({
                "mt_user_id": data["mt_user_id"],
                                }, {
                    "$inc": {
                        "elo": data['elo_value']
                    }, 
                    "$set": {
                     'elo_updated_at': datetime.now()
                    }
                })  for data in mt_user_id_elo.values()
            ]

        
        user_to_updated = []
        
        
        for data in user_id_elo.values():
            
            if self.is_valid_object(data['user_id']):
                update = UpdateOne(
                    {
                        "_id": data["user_id"]
                    },{
                        "$inc": {
                            "elo": data['elo_value']
                        }, 
                        "$set": {
                            'elo_updated_at': datetime.now()
                        }
                    }
                )
        
                user_to_updated.append(update)
        
        print("users_total.............", len(user_to_updated))
        
        
        print("comienza a escribir en tranding_accounts.............")
        TradingAccount().db.collection.bulk_write(
            ta_to_update, ordered=False
        )
        print("comienza a escribir en users.............")
        User().db.collection.bulk_write(
            user_to_updated, ordered=False
        )
        print("termina de escribir en users y trading_accounts.............")
        
        if trades_added > 0:
            operations = [InsertOne(document) for document in trades_to_create]
            
            # split operations in chunks of 1000 and insert
            if len(operations) > 1000:
                for i in range(0, len(operations), 1000):
                    print('inserting trades from ' + str(i) + ' to ' + str(i + 1000))
                    Trade().db.collection.bulk_write(operations[i:i+1000], ordered=False)
            else:
                Trade().db.collection.bulk_write(operations, ordered=False)

            utils.print_with_module(
                "TaskManager.update_trades_collection", "Trades added: " + str(trades_added))

    def get_trades(self, mt_user_id, broker='matchtrader'):
        trades_count = self.get_trades_count(mt_user_id, broker, 'active')
        if trades_count > 0:
            # print ("Found " + str(trades_count) + " trades for user " + str(mt_user_id))
            # trades = self.trades.get_all(str(mt_user_id))
            trades = self.trades.find_by_mt_user_id_and_broker_and_status(
                mt_user_id, broker, 'active')
            # convert to list
            trades = list(trades)

            return trades
        else:
            return []

    def get_trades_count(self, mt_user_id, broker, status='active'):
        # return self.trades.count_documents({'mt_user_id': str(mt_user_id), 'broker': broker, 'status': status})
        return self.trades.count_documents({'mt_user_id': str(mt_user_id), 'broker': broker})

    def update_collections_for_new_system(self):
        res = self.trading_accounts.add_field_to_all_documents(
            'broker', 'matchtrader')
        res2 = self.trades.add_field_to_all_documents('broker', 'matchtrader')
        print("trading_accounts updated: " + str(res.modified_count))
        print("trades updated: " + str(res2.modified_count))

    @utils.task_log('update_trading_accounts_statistics')
    def update_trading_accounts_statistics(self):
        updated_trading_accounts_statistic = 0
        trading_account_to_updated = []
        broker_account = cda.get_broker_accounts_chunked(self)

        trading_accounts_db = cda.get_all_trading_accounts_by_broker_accounts(
            broker_account)

        trading_accounts_data = cda.trading_account_to_set(trading_accounts_db)

        trades_by_user_data = cda.get_trades_actives_by_trading_accounts_set(
            trading_accounts_db)
        for account in broker_account:

            mt_user_id = str(account['clientId'])
            broker = account['broker']

            set_key = mt_user_id + '_' + broker

            trading_account = trading_accounts_data.get(set_key, None)

            if trading_account is not None:
                trades = trades_by_user_data.get(set_key, [])

                # check if trades is a Cursor
                if trades is not None:
                    # print ("trades found for user " + str(account['clientId']) + " : " + str(len(trades)))

                    # update win_rate, total_trades, avg_return_trade
                    total_trades = len(trades)
                    win_rate = 0
                    win_counts = 0
                    loss_counts = 0
                    total_profits = 0
                    for trade in trades:
                        # convert profit (contains $) to float
                        profit = trade['profit']
                        if profit > 0:
                            win_counts += 1
                        elif profit < 0:
                            loss_counts += 1
                        total_profits += profit

                    if total_trades > 0:
                        win_rate = (win_counts / total_trades) * \
                            100 if total_trades > 0 else 0
                        avg_return_trade = total_profits / total_trades if total_trades > 0 else 0
                        data = {
                            'win_rate': win_rate,
                            'total_trades': total_trades,
                            'avg_return_trade': avg_return_trade,
                            'updated_at': datetime.utcnow()
                        }
                        updated_trading_accounts_statistic += 1
                        trading_account_to_updated.append(UpdateOne(
                            {'broker': trading_account['broker'], 'mt_user_id': account['clientId']}, {
                                '$set': data}
                        ))

        if updated_trading_accounts_statistic > 0:
            TradingAccount().db.collection.bulk_write(
                trading_account_to_updated, ordered=False
            )
            utils.print_with_module("TaskManager.update_trading_accounts_statistics", "Updated " + str(
                updated_trading_accounts_statistic) + " trading accounts statistics")

    @utils.task_log('update_wordpress_leaderboard')
    def update_wordpress_leaderboard(self):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        imports_dir = os.path.join(current_dir, '../../../imports')

        if self.chunk_number == 1:
            if self.wordpress_task:
                user_profits, csv = self.generate_csv_leaderboard()
                functions.upload_csv_by_sftp(imports_dir, csv)
                functions.update_wordpress_funds(user_profits)

    def generate_csv_leaderboard(self):
        # get trading_accounts in k3 remarks.
        filter = {
            'status': 'active',
            '$or': [
                {'remarks': {'$regex': 'k3pn'}},
                {'remarks': {'$regex': 'k2pa'}}
            ]
        }
        if os.getenv('TEST_ENVIRONMENT', 'False') == 'True':
            cft_users = self.users.db.collection.find(
                {'Login_name': {'$regex': 'cryptofundtrader.com'}}, {'_id': 1})
            filter = {
                'status': 'active',
                '$or': [
                    {'remarks': {'$regex': 'k3pn'}},
                    {'remarks': {'$regex': 'k2pa'}}
                ],
                'user_id': {'$nin': [u['_id'] for u in cft_users]}
            }

        trading_accounts = self.trading_accounts.find(filter)

        # group trading accounts by user_id. sum account_size_by_remarks and equity property
        users = {}
        user_profits = 0

        for trading_account in trading_accounts:
            user = self.users.find_one_by_id(trading_account['user_id'])

            mt_user_id = trading_account['mt_user_id']
            broker = trading_account['broker']
            id = broker + '_' + mt_user_id

            if functions.is_internal_email(user['User_email']) == False:
                account_size = self.get_account_size_by_remarks(
                    trading_account['remarks'])
                profit = float(trading_account['equity']) - float(account_size)
                users[id] = {
                    'broker': broker,
                    'mt_user_id': mt_user_id,
                    'user_id': trading_account['user_id'],
                    'email': user['User_email'],
                    'name': user['First_name'],
                    'account_size': account_size,
                    'equity': trading_account['equity'],
                    'profit': profit,
                    'created_at': trading_account['created_at'] if 'created_at' in trading_account else datetime.now()
                }
                if len(user['last_name']) > 0:
                    users[id]['name'] += ' ' + user['last_name'][0] + '.'

                users[id]['gain'] = round(
                    (users[id]['profit'] / users[id]['account_size']) * 100, 2)
                if profit > 0:
                    user_profits += profit

        # Convert user fields to Name, Account Size, Equity, Profit, % Gain
        # sort by profit
        users = sorted(users.values(), key=lambda x: x['gain'], reverse=True)

        # convert to csv
        csv = '"Name","Account Size","Equity","Profit","% Gain"' + "\n"
        # generate a new csv line with all fields enclosed by ", and separated by ,
        for user in users:
            user['gain'] = str(user['gain'])
            csv += '"' + user['name'] + '","' + utils.format_to_dollars(user['account_size']) + '","' + utils.format_to_dollars(
                user['equity']) + '","' + utils.format_to_dollars(user['profit']) + '","' + user['gain'] + '%"' + "\n"

        return user_profits, csv

    def get_account_size_by_remarks(self, remarks):
        obj = self.remarks.find_one_by_key(remarks)
        if obj is not None:
            return obj['amount']
        return 0

    def generate_csv_tournament_team(self):
        # cada remarks es un equipo, y necesito: remark, nº de miembros, capital_total, beneficio_total, % de beneficio

        try:
            remarks = list(self.remarks.db.collection.find({'key': {'$in': [
                'demo\\duje_matic',
                'demo\\cryptorisegroup',
                'demo\\ustatrader',
                'demo\\thissdax',
                'demo\\traderomercan',
                'demo\\traderoasis',
                'demo\\marresecira',
                'demo\\luya_madiba',
                'demo\\trader_smaug',
                'demo\\lightofkingdom',
                'demo\\failedtosucceed',
                'demo\\mrraja1326',
                'demo\\mona_trades'
            ]}}, {'key': 1, 'amount': 1}))

            print('remarks', remarks)

            filter = {

                "status": 'active',
                'remarks': {
                    '$in': [
                        'demo\\duje_matic',
                        'demo\\cryptorisegroup',
                        'demo\\ustatrader',
                        'demo\\thissdax',
                        'demo\\traderomercan',
                        'demo\\traderoasis',
                        'demo\\marresecira',
                        'demo\\luya_madiba',
                        'demo\\trader_smaug',
                        'demo\\lightofkingdom',
                        'demo\\failedtosucceed',
                        'demo\\mrraja1326',
                        'demo\\mona_trades'
                    ]
                }
            }
            if os.getenv('TEST_ENVIRONMENT', 'False') == 'True':
                cft_users = self.users.db.collection.find(
                    {'Login_name': {'$regex': 'cryptofundtrader.com'}}, {'_id': 1})
                filter = {
                    "status": 'active',
                    'remarks': {
                        '$in': [
                            'demo\\duje_matic',
                            'demo\\cryptorisegroup',
                            'demo\\ustatrader',
                            'demo\\thissdax',
                            'demo\\traderomercan',
                            'demo\\traderoasis',
                            'demo\\marresecira',
                            'demo\\trader_smaug',
                            'demo\\lightofkingdom',
                            'demo\\failedtosucceed',
                            'demo\\mrraja1326',
                            'demo\\mona_trades'
                        ]
                    },
                    'user_id': {'$nin': [u['_id'] for u in cft_users]}
                }

            result = list(self.trading_accounts.db.collection.aggregate([
                {
                    '$match': filter
                },
                {
                    '$group': {
                        "_id": "$remarks",
                        "total_of_members": {"$sum": 1},
                        "total_balance": {"$sum": "$balance"},
                    }
                }
            ]))

            csv = '"Rank","Team","Total of members","Capital","Profit","Profit %"' + "\n"
            print('result', result)
           
            result_data = []
            # generate a new csv line with all fields enclosed by ", and separated by ,
            i = 0
            for team in result:
                total_of_members = team['total_of_members']
                remark = next(
                    (rm for rm in remarks if rm['key'] == team['_id']), None)
                print('remark', remark)
                print('team[_id]', team['_id'])
                total_capital = remark['amount'] * total_of_members
                total_balance = team['total_balance'] - total_capital

                remark_title = remark['key'].replace('demo\\duje_matic', 'Duje Matic').replace('demo\\cryptorisegroup', 'Cryptorise Group').replace('50k2pnfee', '50 k2pn Fee').replace('demo\\ustatrader', 'Ustatrader').replace(
                    'demo\\thissdax', 'Thissdax').replace('demo\\traderomercan', 'Traderomercan').replace('demo\\traderoasis', 'Traderoasis').replace('demo\\marresecira', 'Marresecira').replace('demo\\', '')
                profit_percentage = round(
                    total_balance * 100 / total_capital, 2)
                result_data.append({
                    'remark_title': remark_title,
                    'total_of_members': total_of_members,
                    'total_capital': total_capital,
                    'total_balance': total_balance,
                    'profit_percentage': profit_percentage
                })

            # sort by profit%
            result = sorted(
                result_data, key=lambda x: x['profit_percentage'], reverse=True)
            # i =
            for team in result:
                i = i + 1

                rank = str(i)

                csv += '"' + rank + '","' + team['remark_title'] + '","' + str(team['total_of_members']) + '","' + utils.format_to_dollars(
                    team['total_capital']) + '","' + utils.format_to_dollars(team['total_balance']) + '","' + str(team['profit_percentage']) + '%"' + "\n"

            print(csv)
            return csv

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print(e)

    @utils.task_log('update_wordpress_tournament_team')
    def update_wordpress_tournament_team(self):
        current_dir = os.path.dirname(os.path.realpath(__file__))

        imports_dir = os.path.join(current_dir, '../../../imports')

        if self.chunk_number == 1:
            if self.wordpress_task:
                csv = self.generate_csv_tournament_team()
                print(csv)
                functions.upload_csv_by_sftp(
                    imports_dir, csv, 'tournament_team.csv')

    def generate_csv_leaderboard_teams(self):
        # get trading_accounts in k3 remarks.
        filter = {
            
            'remarks': {
                '$in': [
                    'demo\\duje_matic',
                    'demo\\cryptorisegroup',
                    'demo\\ustatrader',
                    'demo\\thissdax',
                    'demo\\traderomercan',
                    'demo\\traderoasis',
                    'demo\\marresecira',
                    'demo\\luya_madiba',
                    'demo\\trader_smaug',
                    'demo\\lightofkingdom',
                    'demo\\failedtosucceed',
                    'demo\\mrraja1326',
                    'demo\\mona_trades'
                ]
            }
        }
        if os.getenv('TEST_ENVIRONMENT', 'False') == 'True':
            cft_users = self.users.db.collection.find(
                {'Login_name': {'$regex': 'cryptofundtrader.com'}}, {'_id': 1})

            filter = {
                
                'remarks': {
                    '$in': [
                        'demo\\duje_matic',    
                        'demo\\cryptorisegroup',
                        'demo\\ustatrader',
                        'demo\\thissdax',
                        'demo\\traderomercan',
                        'demo\\traderoasis',
                        'demo\\marresecira',
                        'demo\\luya_madiba',
                        'demo\\trader_smaug',
                        'demo\\lightofkingdom',
                        'demo\\failedtosucceed',
                        'demo\\mrraja1326',
                        'demo\\mona_trades'
                    ],
                    'user_id': {'$nin': [u['_id'] for u in cft_users]}
                }

            }
        trading_accounts = self.trading_accounts.find(filter)

        # group trading accounts by user_id. sum account_size_by_remarks and equity property
        users = {}
        user_profits = 0
        i = 0
        for trading_account in trading_accounts:
            user = self.users.find_one_by_id(trading_account['user_id'])
            if not user:
                continue
            mt_user_id = trading_account['mt_user_id']
            broker = trading_account['broker']
            id = broker + '_' + mt_user_id

            if functions.is_internal_email(user['User_email']) == False:
                account_size = self.get_account_size_by_remarks(
                    trading_account['remarks'])
                profit = float(trading_account['equity']) - float(account_size)
                i = i + 1
                users[id] = {
                    'rank': str(i) if trading_account['status'] == 'active' else 'DQ',
                    'broker': broker,
                    'mt_user_id': mt_user_id,
                    'user_id': trading_account['user_id'],
                    'email': user['User_email'],
                    'name': user['First_name'],
                    'account_size': account_size,
                    'equity': trading_account['equity'],
                    'profit': profit,
                    'created_at': trading_account['created_at'] if 'created_at' in trading_account else datetime.now()
                }
                if len(user['last_name']) > 0:
                    users[id]['name'] += ' ' + user['last_name'][0] + '.'

                users[id]['gain'] = round(
                    (users[id]['profit'] / users[id]['account_size']) * 100, 2)
                if profit > 0:
                    user_profits += profit

        # filter out users with DQ rank
        users_active = {k: v for k, v in users.items() if v['rank'] != 'DQ'}

        # assign rank number by gain
        users_active = sorted(users_active.values(),
                              key=lambda x: x['gain'], reverse=True)
        for i, user in enumerate(users_active):
            user['rank'] = str(i + 1)
            user['gain'] = str(user['gain'])

        users_closed = {k: v for k, v in users.items() if v['rank'] == 'DQ'}
        
        for user_id, user_data in users_closed.items():
            user_data['created_at'] = self.parse_created_at(user_data['created_at'])
        
        users_closed = sorted(users_closed.values(),
                              key=lambda x: x['created_at'])

        # convert to csv
        csv = '"Rank","Name","Account Size","Equity","Profit","% Gain"' + "\n"
        # generate a new csv line with all fields enclosed by ", and separated by ,
        for user in users_active:
            csv += '"' + user['rank'] + '","' + user['name'] + '","' + utils.format_to_dollars(user['account_size']) + '","' + utils.format_to_dollars(
                user['equity']) + '","' + utils.format_to_dollars(user['profit']) + '","' + user['gain'] + '%"' + "\n"

        for user in users_closed:
            csv += '"' + user['rank'] + '","' + user['name'] + '","' + \
                utils.format_to_dollars(
                    user['account_size']) + '","-","-","-"' + "\n"

        return user_profits, csv

    def update_wordpress_tournament_leaderboard(self):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        imports_dir = os.path.join(current_dir, '../../../imports')

        if self.chunk_number == 1:
            if self.wordpress_task:
                user_profits, csv = self.generate_csv_leaderboard_teams()
                functions.upload_csv_by_sftp(
                    imports_dir, csv, 'leaderboard_tournament_team.csv')
                # functions.update_wordpress_funds(user_profits)

    @utils.task_log('update_wordpress_tournament')
    def update_wordpress_tournament(self):
        current_dir = os.path.dirname(os.path.realpath(__file__))
        imports_dir = os.path.join(current_dir, '../../../imports')

        if self.chunk_number == 1:
            if self.wordpress_task:
                user_profits, csv = self.generate_csv_tournament()
                functions.upload_csv_by_sftp(
                    imports_dir, csv, 'tournament.csv')
    


    def parse_created_at(self, created_at):
        if isinstance(created_at, str):
            try:
                return datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                try:
                    return datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return datetime.min 
        return created_at or datetime.min 



    def generate_csv_tournament(self):
        filter = {
            'remarks': {'$regex': 'tournament'}
        }

        if os.getenv('TEST_ENVIRONMENT', 'False') == 'True':
            cft_users = self.users.db.collection.find(
                {'Login_name': {'$regex': 'cryptofundtrader.com'}}, {'_id': 1})
            filter = {
                'remarks': {'$regex': 'tournament'},
                'user_id': {'$nin': [u['_id'] for u in cft_users]}
            }
        trading_accounts = self.trading_accounts.find(filter)

        # group trading accounts by user_id. sum account_size_by_remarks and equity property
        users = {}
        user_profits = 0

        for trading_account in trading_accounts:
            user = self.users.find_one_by_id(trading_account['user_id'])

            mt_user_id = trading_account['mt_user_id']
            broker = trading_account['broker']
            id = broker + '_' + mt_user_id

            if functions.is_internal_email(user['User_email']) == False:
                account_size = self.get_account_size_by_remarks(
                    trading_account['remarks'])
                profit = float(trading_account['equity']) - float(account_size)
                users[id] = {
                    'rank': '' if trading_account['status'] == 'active' else 'DQ',
                    'broker': broker,
                    'mt_user_id': mt_user_id,
                    'user_id': trading_account['user_id'],
                    'email': user['User_email'],
                    'name': user['First_name'],
                    'account_size': account_size,
                    'equity': trading_account['equity'],
                    'profit': profit,
                    'created_at': trading_account['created_at'] if 'created_at' in trading_account else datetime.now()
                }
                if len(user['last_name']) > 0:
                    users[id]['name'] += ' ' + user['last_name'][0] + '.'

                users[id]['gain'] = round(
                    (users[id]['profit'] / users[id]['account_size']) * 100, 2)
                if profit > 0:
                    user_profits += profit

        # filter out users with DQ rank
        users_active = {k: v for k, v in users.items() if v['rank'] != 'DQ'}

        # assign rank number by gain
        users_active = sorted(users_active.values(),
                              key=lambda x: x['gain'], reverse=True)
        for i, user in enumerate(users_active):
            user['rank'] = str(i + 1)
            user['gain'] = str(user['gain'])

        users_closed = {k: v for k, v in users.items() if v['rank'] == 'DQ'}
        
        for user_id, user_data in users_closed.items():
            user_data['created_at'] = self.parse_created_at(user_data['created_at'])
            
        users_closed = sorted(users_closed.values(),
                              key=lambda x: x['created_at'])

        # convert to csv
        csv = '"Rank","Name","Account Size","Equity","Profit","% Gain"' + "\n"
        # generate a new csv line with all fields enclosed by ", and separated by ,
        for user in users_active:
            csv += '"' + user['rank'] + '","' + user['name'] + '","' + utils.format_to_dollars(user['account_size']) + '","' + utils.format_to_dollars(
                user['equity']) + '","' + utils.format_to_dollars(user['profit']) + '","' + user['gain'] + '%"' + "\n"

        for user in users_closed:
            csv += '"' + user['rank'] + '","' + user['name'] + '","' + \
                utils.format_to_dollars(
                    user['account_size']) + '","-","-","-"' + "\n"

        return user_profits, csv

    def update_trading_accounts_balances(self, all_accounts):

        cda.print_chunk_data(self, "UPDATE_TRADING_ACCOUNTS_BALANCES")

        datemilliseconds = datetime.now()

        updated_trading_accounts = 0
        updated_trading_accounts_statistic = 0
        updated_trading_accounts_days_traded = 0
        created_balances = 0

        trading_accounts_to_update = []
        balances_to_create = []

        broker_accounts = cda.get_broker_accounts_all_filter(
            self) if all_accounts else cda.get_broker_accounts_chunked_filter(self)

        if all_accounts:
            broker_accounts = cda.get_broker_accounts_without_todays_balance(
                broker_accounts)

        trading_accounts_data = cda.get_trading_accounts_by_broker_accounts_active_pending_withdrawal_set(
            broker_accounts)

        all_trades_count = cda.get_last_trades_count_by_broker_accounts_set(
            self, broker_accounts)

        all_yesterday_count = cda.get_yesterday_trades_count_by_broker_accounts_set(
            self, broker_accounts)

        balances_data = cda.get_today_balances_by_broker_accounts_set(
            broker_accounts)

        for account in broker_accounts:
            mt_user_id = str(account['clientId'])
            broker = account['broker']

            date_year_month_day = datetime.utcnow().strftime('%Y-%m-%d')

            set_key = mt_user_id + '_' + broker  # 12357_metrader

            trading_account = trading_accounts_data.get(set_key, None)

            if trading_account is not None:
                # Solo se permite si está activa
                balance = utils.adapt_integer_amount_to_decimal_amount(
                    float(account['balance']))

                utils.print_with_module("TaskManager.update_users_statistics",
                                        "broker balance for user " + mt_user_id + " is " + str(balance))

                trades_count = all_trades_count.get(set_key, 0)

                yesterday_trades_count = all_yesterday_count.get(set_key, 0)

                print('trades_count', trades_count)

                print('yesterday_trades_count', yesterday_trades_count)

                utils.print_with_module("TaskManager.update_users_statistics", "User " + mt_user_id + " has " + str(
                    trades_count) + " total trades and " + str(yesterday_trades_count) + " yesterday trades")

                new_days_traded = trading_account['days_traded'] + \
                    1 if yesterday_trades_count > 0 else trading_account['days_traded']

                new_withdrawal_days_traded = trading_account['withdrawal_days_traded'] + \
                    1 if yesterday_trades_count > 0 else trading_account['withdrawal_days_traded']

                ta_update_data = {
                    'total_trades': trades_count,
                    'start_balance': balance,
                    'days_traded': new_days_traded,
                    'withdrawal_days_traded': new_withdrawal_days_traded,
                    'updated_at': datetime.utcnow()
                }

                if yesterday_trades_count > 0:
                    updated_trading_accounts_days_traded += 1

                utils.print_with_module("TaskManager.update_users_statistics",
                                        "Updating trading account " + mt_user_id + " with ta_update_data " + str(ta_update_data))
                updated_trading_accounts += 1

                trading_accounts_to_update.append(
                    UpdateOne({'broker': broker, 'mt_user_id': mt_user_id}, {'$set': ta_update_data}))

                # Identifier
                utils.print_with_module("TaskManager.update_users_statistics",
                                        "Checking balance for user " + mt_user_id + " and date " + date_year_month_day)
                filter = {
                    'mt_user_id': str(mt_user_id),
                    'date_year_month_day': date_year_month_day
                }

                utils.print_with_module(
                    "TaskManager.update_users_statistics", filter)

                balance_key = mt_user_id + '_' + date_year_month_day

                obj = balances_data.get(balance_key, None)

                utils.print_with_module(
                    "TaskManager.update_users_statistics", obj)

                utils.print_with_module("TaskManager.update_users_statistics", "balance for user " +
                                        mt_user_id + " and date " + date_year_month_day + " is " + str(obj))
                if obj is None:
                    utils.print_with_module("TaskManager.update_users_statistics", "creating balance for user " + str(
                        trading_account['user_id']) + " and date " + date_year_month_day)
                    # add balance always
                    balance_data_to_create = {
                        'user_id': trading_account['user_id'],
                        'broker': trading_account['broker'],
                        'mt_user_id': mt_user_id,
                        'trading_account_id': trading_account['_id'],
                        'balance': balance,
                        'date_year_month_day': date_year_month_day,
                        'created_at': datetime.utcnow()
                    }
                    utils.print_with_module("TaskManager.update_users_statistics",
                                            "Adding balance for user " + mt_user_id + " with balance_data_to_create " + str(balance_data_to_create))

                    balances_to_create.append(balance_data_to_create)
                    created_balances += 1
                else:
                    utils.print_with_module("TaskManager.update_users_statistics", "balance exists for user " + str(
                        trading_account['user_id']) + " and date " + date_year_month_day)

        if len(trading_accounts_to_update) > 0:
            self.trading_accounts.db.collection.bulk_write(
                trading_accounts_to_update, ordered=False)
            print('vas a actualizar/crear ',
                  str(len(trading_accounts_to_update)))

        if len(balances_to_create) > 0:
            try:
                operations = [self.balances.db.collection.insert_one(
                    document) for document in balances_to_create]
            except Exception as e:
                print('Error in update_trading_accounts_balances')
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
                print(e)
            # self.balances.db.collection.bulk_write(operations, ordered=False)
            print('vas a crear ', str(len(balances_to_create)))

        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print('finished update_trading_accounts_balances: ',
              'time' + str(datetime.now() - datemilliseconds))
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

        if updated_trading_accounts > 0:
            utils.print_with_module("TaskManager.update_users_statistics",
                                    "Updated " + str(updated_trading_accounts) + " trading accounts")
        if updated_trading_accounts_statistic > 0:
            utils.print_with_module("TaskManager.update_users_statistics", "Updated " + str(
                updated_trading_accounts_statistic) + " trading accounts statistics")
        if updated_trading_accounts_days_traded > 0:
            utils.print_with_module("TaskManager.update_users_statistics", "Updated " + str(
                updated_trading_accounts_days_traded) + " trading accounts days traded")
        if created_balances > 0:
            utils.print_with_module(
                "TaskManager.update_users_statistics", "Created " + str(created_balances) + " balances")

    @utils.task_log('update_users_statistics_users_not_updated')
    def update_users_statistics_users_not_updated(self):
        if self.chunk_number == 1:
            # update/create balances for trading accounts
            self.update_trading_accounts_balances(True)
            # Run update users daily drawdown
            self.update_users_daily_drawdown(True)

    @utils.task_log('update_users_statistics')
    def update_users_statistics(self):
        # update/create balances for trading accounts
        self.update_trading_accounts_balances(False)  # a, b, c
        # Run update users daily drawdown
        self.update_users_daily_drawdown(False)     # c,d,e

    @utils.task_log('update_users_daily_drawdown')
    def update_users_daily_drawdown(self, all_accounts):
        cda.print_chunk_data(self, "UPDATE_USERS_DAILY_DRAWDOWN")
        trading_accounts_to_update = []

        datemilliseconds = datetime.now()

        broker_accounts = cda.get_broker_accounts_chunked_filter(self)

        if all_accounts:
            broker_accounts = cda.get_broker_accounts_without_todays_updated_at_daily_drawdown(
                broker_accounts)

        current_timestamp_since_1970 = int(time.time())
        updated = 0

        trading_accounts_data = cda.get_trading_accounts_by_broker_accounts_active_pending_withdrawal_set(
            broker_accounts)

        balances_data = cda.get_yesterday_balances_by_broker_accounts_set(
            broker_accounts)

        yesterday_date_year_month_day = (
            datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')


# [0,...,39]  0-9 10-19 20-29 30-39
# [0,...,41]  0-10 11-21 22-31 32-41
        for account in broker_accounts:
            clientInfo = account['clientInfo']
            email = clientInfo['email']
            if email is not None and len(email) > 0:
                registration_time_stamp = int(
                    account['clientInfo']['registrationDate']) / 1000
                trading_account = trading_accounts_data.get(
                    account['clientId'] + '_' + account['broker'], None)

                if trading_account is not None and 'remarks' in trading_account and trading_account['remarks']:
                    last_balance = balances_data.get(
                        account['clientId'] + '_' + yesterday_date_year_month_day, None)

                    is_new = (current_timestamp_since_1970 -
                              registration_time_stamp) < (24 * 60 * 60)
                    balance = utils.adapt_integer_amount_to_decimal_amount(
                        int(account['balance']))
                    limits = self.get_account_limits_by_broker(
                        is_new, trading_account['remarks'], balance, trading_account['broker'])
                    daily_drawdown = limits['daily_drawdown']

                    # if is_new == False:
                    start_balance = float(
                        trading_account['start_balance'])

                    remarks = trading_account['remarks']
                    if 'pa' in remarks:
                        daily_drawdown = start_balance * 0.96
                    else:
                        daily_drawdown = start_balance * 0.95

                    filter = {
                        'broker': trading_account['broker'],
                        'mt_user_id': str(account['clientId'])
                    }
                    trading_accounts_to_update_data = {
                        'daily_drawdown': daily_drawdown,
                        'updated_at': datetime.utcnow(),
                        'updated_at_daily_drawdown': datetime.utcnow(),

                    }
                    # self.trading_accounts.update_one(filter, trading_accounts_to_update_data)
                    trading_accounts_to_update.append(
                        UpdateOne(filter, {'$set': trading_accounts_to_update_data}))
                    updated += 1
                    utils.print_with_module("TaskManager.update_users_daily_drawdown",
                                            "daily_drawdown updated for user " + account['clientId'], 'mt_user_id', account['clientId'])

        if updated > 0:
            self.trading_accounts.db.collection.bulk_write(
                trading_accounts_to_update,
                ordered=False

            )
            utils.print_with_module("TaskManager.update_users_daily_drawdown",
                                    "daily_drawdown updated for " + str(updated) + " users")
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print('finished update_users_daily_drawdown: ',
              'time' + str(datetime.now() - datemilliseconds))
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

    def check_and_update_account_limits(self, trading_account, last_balance):
        if trading_account['remarks'] != 'preliminary':
            # get current start_balance
            start_balance = float(trading_account['start_balance'])
            previous_balance = None
            if last_balance is not None:
                previous_balance = float(last_balance['balance'])
            else:
                # use remarks if no balance before
                previous_balance = self.get_start_balance_by_remarks(
                    trading_account['remarks'])

            utils.print_with_module("TaskManager.check_account_limits", "start_balance for trading_account " + str(
                trading_account['mt_user_id']) + " is " + str(start_balance) + " previous_balance: " + str(previous_balance))
            # if difference between start_balance and last_balance.balance if greater than 10k, withdraw difference from broker
            difference = float(start_balance) - float(previous_balance)
            # If amount if greater than 10k except for tournament accounts

            if difference > 10000:
                # withdraw difference
                difference = difference - 10000

                self.broker_manager.withdrawal(
                    trading_account['mt_user_id'], difference, trading_account['broker'], 'Daily profit limit withdrawal')

                # update start_balance in trading_account
                utils.print_with_module("TaskManager.check_account_limits", "withdraw money updated for trading_account " + str(
                    trading_account['mt_user_id']) + " difference: " + str(difference))
                utils.send_telegram_message(self.telegrams_prefix + "Withdrawal of " + str(difference) + " for " + str(
                    trading_account['mt_user_id']) + " due to daily 10k account limit")
                return True
        return False

    def get_start_balance_by_remarks(self, remarks):
        # remarks could be 10k, 25k, 50k, 100k, 200k
        # remove demo\\
        remarks = remarks.replace('demo\\', '')
        # split by k, multiply by 1000
        return int(remarks.split('k')[0]) * 1000

    def get_yesterday_trades_count(self, mt_user_id, broker='matchtrader'):
        # Generate previous date in mm/dd/yyyy format
        yesterday = datetime.now() - timedelta(days=1)
        yesterday = yesterday.strftime("%m/%d/%Y")
        # return self.trades.count_documents({'status': 'active', 'mt_user_id': str(mt_user_id), 'close_time': {"$eq": yesterday}, 'broker': broker})
        return self.trades.count_documents({'mt_user_id': str(mt_user_id), 'close_time': {"$eq": yesterday}, 'broker': broker})

    @utils.task_log('check_data')
    def check_data(self):
        # check trading_accounts exist on brokers
        self.check_trading_accounts()

    def check_trading_accounts(self):
        # Si esta activo, pero no hay entrada en broker, error
        trading_accounts_total = self.trading_accounts.count_documents(
            {
                'status': {
                    '$in': ['active', 'pending_withdrawal']
                }
            }
        )
        limit = int(trading_accounts_total / self.chunk_size)

        trading_accounts = list(self.trading_accounts.db.collection.find(
            {
                'status': {
                    '$in': ['active', 'pending_withdrawal']
                }
            }
        ).limit(limit).skip(limit * self.chunk_number))

        accounts_not_found = []
        for ta in trading_accounts:
            # print (ta)
            broker_account = self.broker_manager.get_account(
                ta['mt_user_id'], ta['broker'])
            if broker_account is None:
                utils.print_with_module("TaskManager.check_data", "Broker account not found for trading_account " + str(
                    ta['mt_user_id']) + " broker: " + ta['broker'])
                accounts_not_found.append(
                    {'broker': ta['broker'], 'mt_user_id': ta['mt_user_id']})

        if len(accounts_not_found) > 0:
            # join all accounts_not_found. group every ten accounts and send telegram
            accounts_not_found = [accounts_not_found[i:i + 10]
                                  for i in range(0, len(accounts_not_found), 10)]
            for accounts in accounts_not_found:
                utils.print_with_module(
                    "TaskManager.check_data", "Sending telegram message for accounts: " + str(accounts))
                # utils.send_telegram_error_message("Accounts not found in broker: " + str(accounts))

    @utils.task_log('update_pending_withdrawals')
    def update_pending_withdrawals(self):
        """
        Recover all trading_accounts with status pending_withdrawal
        Unlock accounts in broker
        Change trading_accounts status to active
        """
        trading_accounts = self.trading_accounts.find_by_status(
            'pending_withdrawal')
        trading_accounts = list(trading_accounts)
        for trading_account in trading_accounts:
            mt_user_id = trading_account['mt_user_id']
            broker = trading_account['broker']
            # utils.print_with_module("TaskManager.update_pending_withdrawal", "Unlocking account for user " + mt_user_id)
            self.broker_manager.unlock_account(mt_user_id, broker)
            # utils.print_with_module("TaskManager.update_pending_withdrawal", "Unlocking account for user " + mt_user_id + " done")
            self.trading_accounts.set_active(mt_user_id, broker)

    @utils.task_log('check_trades_too_fast')
    def check_trades_too_fast(self):
        # get last week trades
        current_date = datetime.now()
        # find last week trades by close_time (month/day/year format) and duration less than 30 seconds
        filter = {
            'created_at': {'$gte': current_date - timedelta(days=7)},
            'status': 'active',
            'duration': {'$lt': 30}
        }
        last_week_trades = self.trades.find_by_filter(filter)
        mt_user_ids = []
        for trade in last_week_trades:
            if trade['mt_user_id'] not in mt_user_ids:
                mt_user_ids.append(
                    {'mt_user_id': trade['mt_user_id'], 'broker': trade['broker']})

        users_data = []
        for mt_user_id in mt_user_ids:
            # ta = self.trading_accounts.get_by_id(mt_user_id)
            ta = self.trading_accounts.find_one_by_mt_user_id_and_broker(
                mt_user_id['mt_user_id'], mt_user_id['broker'])
            if ta is not None and 'status' in ta and ta['status'] == 'active':
                user = self.users.find_one_by_id(ta['user_id'])
                if user:
                    users_data.append({
                        'email': utils.get_email_by_environment(user['User_email']),
                        'firstname': user['First_name'],
                        'lastname': user['last_name'],
                        'user_id': user['_id'],
                        'mt_user_id': mt_user_id
                    })

        if len(users_data) > 0:
            utils.print_with_module("TaskManager.check_trades_too_fast",
                                    "trades too fast detected for users: " + str(users_data))
            if utils.is_n8n_webhook_enabled() and self.send_n8n_notifications:
                # call n8n webhook to notify admin
                url = 'https://n8n.cr.cryptofundtrader.com/webhook/trades-too-fast-detected'
                headers = {
                    'Content-Type': 'application/json'
                }
                data = {
                    'users': users_data,
                    'app_url': utils.get_app_url_by_environment()
                }
                response = utils.do_post_request(url, headers, data)
                if response.status_code != 200:
                    utils.print_with_module("TaskManager.check_trades_too_fast",
                                            "trades too fast webhook error " + str(response.status_code))
                else:
                    utils.print_with_module(
                        "TaskManager.check_trades_too_fast", "trades too fast webhook called")

    @utils.task_log('check_if_equity_is_over_10k')
    def check_if_equity_is_over_10k(self):
        try:
            broker_accounts = cda.get_broker_accounts_chunked(self)

            trading_accounts = cda.get_all_trading_accounts_by_broker_accounts_set(
                broker_accounts)
            affected_accounts = []

            for account in broker_accounts:

                mt_user_id = str(account['clientId'])
                broker = account['broker']
                set_key = mt_user_id + '_' + broker
                trading_account = trading_accounts.get(set_key, None)
                if trading_account is None or not trading_account['start_balance'] or trading_account['status'] == 'closed':
                    continue

                current_equity = utils.adapt_integer_amount_to_decimal_amount(
                    int(account['equity']))

                account_start_balance = trading_account['start_balance']
                diference = current_equity - account_start_balance
                if diference > 10000:

                    affected_accounts.append(
                        {"trading_account": trading_account, "account": account})
                    # send_telegram_message_with_title('Esta cuenta va a cerrar sus trades', 'La cuenta ' + str(account['clientId']) + ' va a cerrar sus trades porque ha superado los 10k de beneficio.')

                    print('account_start_balance', account['clientId'])
            if len(affected_accounts):
                users = []
                for affected in affected_accounts:
                    account = affected['account']
                    trading_account = affected['trading_account']
                    current_equity = utils.adapt_integer_amount_to_decimal_amount(
                        int(account['equity']))
                    account_start_balance = trading_account['start_balance']
                    diference = current_equity - account_start_balance

                    self.broker_manager.close_open_orders(
                        account['clientId'], account['broker'])
                    self.broker_manager.close_open_positions(
                        account['clientId'], account['broker'])
                    self.broker_manager.forze_withdrawal(
                        trading_account['mt_user_id'], diference - 10000, trading_account['broker'], 'Daily profit limit withdrawal')

                    data = {
                        "email": account['clientInfo']['email'],
                        "user_id": str(trading_account['user_id']),
                        "mt_user_id": trading_account['mt_user_id'],
                        "broker": trading_account['broker'],
                        "equity":  current_equity,
                        "start_balance": trading_account['start_balance'],
                        "firstname": account['clientInfo']['name'],
                    }
                    users.append(data)
                if utils.is_n8n_webhook_enabled() and self.send_n8n_notifications:
                    # call n8n webhook to notify admin
                    url = 'https://n8n.cr.cryptofundtrader.com/webhook/10k-rule'
                    headers = {
                        'Content-Type': 'application/json'
                    }
                    data = {
                        'users': users,
                        'app_url': utils.get_app_url_by_environment()
                    }
                    response = utils.do_post_request(url, headers, data)
                    if response.status_code != 200:
                        utils.print_with_module("TaskManager.check_trades_too_fast",
                                                "trades too fast webhook error " + str(response.status_code))
                    else:
                        utils.print_with_module(
                            "TaskManager.check_trades_too_fast", "trades too fast webhook called")

        except Exception as e:
            print('Error in check_if_equity_is_over_10k')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print(e)

    def milliseconds_to_time(self, ms):

        delta = timedelta(milliseconds=ms)

        time = datetime(1, 1, 1)

        time += delta

        return time.strftime("%H:%M:%S")

    def generate_zero_response(self, trading_account):
        zero_response = {
            "mt_user_id": trading_account['mt_user_id'],
            "broker": trading_account['broker'],
            "user_id": trading_account['user_id'],
            "balances": {
                'labels': [],
                'datasets': []
            },
            "remark": {},
            "trades_data": {
                "total_trades": 0,
                "total_win": 0,
                "total_loss": 0,
                "risk_average": 0,
                "risk_percentage": 0,
                "trades_per_day_average": 0,
                "ratio_win_loss": 0,
                "total_win_percentage": 0,
                "win_buy_percentage": 0,
                "win_sell_percentage": 0,
                "instruments_traded": 0,
                "avg_lots": 0,
                "time_open_trade": 0,
                "commission_trade": 0
            },
            "max_drawdown_percent": 0,
            "balance_info": {
                "min_balance": 0,
                "min_balance_day": "",
                "days_to_recover_initial_balance": 0,
                "days_since_min_balance": 0
            },
            "symbols_data": {
                "total_instruments_traded": []
            }
        }
        return zero_response
    
    
    def get_metadata_by_user(self, trading_account):

        if trading_account is None:
            return {"error": "No se encontró la cuenta de trading"}, 404

        print(trading_account['mt_user_id'])

        balances = list(self.balances.find(
            {'mt_user_id': trading_account['mt_user_id']}).sort('date_year_month_day', pymongo.ASCENDING))

        if not balances:
            return self.generate_zero_response(trading_account)

        remark = self.remarks.db.collection.find_one(
            {'key': trading_account['remarks']})
        if remark is None:
            return

        print(remark)

        trades = list(self.trades.db.collection.find(
            {'mt_user_id': trading_account['mt_user_id']}))

        total_trades = len(trades)

        if total_trades == 0:
            return self.generate_zero_response(trading_account)
        else:
            trades_info = {
                "trades_per_day_average": {

                },
                "avg_lots": 0,
                'av_duration': 0,
                "total_trades": total_trades,
                "win_count": 0,
                "loss_count": 0,
                "total_loss_value": 0,
                "avg_commission": 0,
                "win_count_buy": 0,
                "win_count_sell": 0,
                "symbols_traded": {

                }
            }

            for trade_data in trades:
                trades_info["win_count"] += 1 if trade_data["profit"] > 0 else 0
                trades_info["loss_count"] += 1 if trade_data["profit"] < 0 else 0
                trades_info["total_loss_value"] += trade_data["profit"] if trade_data["profit"] < 0 else 0
                trades_info["win_count_buy"] += 1 if trade_data["direction"] == "buy" and trade_data["profit"] > 0 else 0
                trades_info["win_count_sell"] += 1 if trade_data["direction"] == "sell" and trade_data["profit"] > 0 else 0
                trades_info['avg_lots'] += trade_data['lots']
                trades_info['av_duration'] += trade_data['duration']
                if 'commission' in trade_data:
                    trades_info['avg_commission'] += trade_data['commission']

                if trade_data['open_time'] in trades_info['trades_per_day_average']:
                    trades_info['trades_per_day_average'][trade_data['open_time']] += 1
                else:
                    trades_info['trades_per_day_average'][trade_data['open_time']] = 1

                if trade_data['symbol'] in trades_info['symbols_traded']:
                    trades_info['symbols_traded'][trade_data['symbol']
                                                  ]['count'] += 1
                    trades_info['symbols_traded'][trade_data['symbol']
                                                  ]['avg_lots'] += trade_data['lots']
                    trades_info['symbols_traded'][trade_data['symbol']]['percent'] = round(
                        trades_info['symbols_traded'][trade_data['symbol']]['count'] / total_trades * 100, 2)
                else:
                    trades_info['symbols_traded'][trade_data['symbol']] = {
                        "count": 1,
                        "avg_lots": trade_data['lots'],
                        "percent": round(1 / total_trades * 100, 2),
                        "symbol": trade_data['symbol']
                    }

            print("trades_info['avg_commission'] ",
                  trades_info['avg_commission'], trades_info['avg_lots'])
            trades_info['avg_lots'] = round(
                trades_info['avg_lots'] / total_trades, 2)

            trades_info['av_duration'] = self.milliseconds_to_time(
                round(trades_info['av_duration'] / total_trades, 2))

            trades_info['avg_commission'] = round(
                trades_info['avg_commission'] / total_trades, 2)
            print("trades_info['avg_commission'] ",
                  trades_info['avg_commission'])

            trades_info['symbols_traded'] = list(
                trades_info['symbols_traded'].values())
            trades_info['trades_per_day_average'] = round(sum(
                trades_info['trades_per_day_average'].values()) / len(trades_info['trades_per_day_average']), 2)
            print(trades_info['trades_per_day_average'])
            win_count = round(trades_info['win_count'], 2)

            loss_count = round(trades_info['loss_count'], 2)

            total_buy = round(trades_info['win_count_buy'], 2)

            total_sell = round(trades_info['win_count_sell'], 2)

            total_trades = round(trades_info['total_trades'], 2)

            total_loss_value = round(trades_info['total_loss_value'], 2)
            print("total loss................", total_loss_value)

            risk_average = abs(round(
                total_loss_value / loss_count if loss_count != 0 else 0, 2))
            print("risk...........", risk_average)

            risk_percentage = abs(round(
                risk_average / remark['amount'] * 100, 2))

            ratio_win_loss = abs(round(
                trades_info['win_count'] / trades_info['loss_count'], 2) if trades_info['loss_count'] != 0 else 0)
            total_win_percentage = round(
                trades_info['win_count'] / total_trades * 100, 2) if total_trades != 0 else 0
            win_buy_percentage = round(
                trades_info['win_count_buy'] / win_count * 100, 2) if win_count != 0 else 0
            win_sell_percentage = round(
                trades_info['win_count_sell'] / win_count * 100, 2) if win_count != 0 else 0

            max_drawdown_percent = trading_account["min_balance"] / \
                remark["amount"] * 100
            print(max_drawdown_percent)

            min_balance = remark["amount"]
            min_balance_day = None
            days_to_recover_initial_balance = 0
            days_since_min_balance = 0
            i = 0
            low_day_index = 0
            for balance_data in balances:
                i += 1
                if balance_data["balance"] < min_balance:
                    low_day_index = i
                    min_balance = balance_data["balance"]
                    min_balance_day = datetime.strptime(
                        balance_data["date_year_month_day"], '%Y-%m-%d')
                    continue

            if min_balance < remark["amount"]:
                balances = list(balances)
                for balance_data in balances[low_day_index:]:
                    days_since_min_balance += 1
                    if balance_data["balance"] >= remark["amount"]:
                        days_to_recover_initial_balance = (datetime.strptime(
                            balance_data["date_year_month_day"], '%Y-%m-%d') - min_balance_day).days
                        break

            else:
                days_to_recover_initial_balance = 0
                days_since_min_balance = 0

            labels = [
                balance_data["date_year_month_day"] for balance_data in balances]
            datasets = [
                {
                    "label": "Balance",
                    "backgroundColor": "rgba(75,192,192,0.2)",
                    "borderColor": "rgba(75,192,192,1)",
                    "borderWidth": 1,
                    "hoverBackgroundColor": "rgba(75,192,192,0.4)",
                    "hoverBorderColor": "rgba(75,192,192,1)",
                    "data": [balance_data["balance"] for balance_data in balances]
                }
            ]
            response = {
                'labels': labels,
                'datasets': datasets
            }

            labels_instruments = [symbol['symbol']
                                  for symbol in trades_info['symbols_traded']]
            data_instruments = [symbol['count']
                                for symbol in trades_info['symbols_traded']]

            transformed_data = {
                'labels': labels_instruments,
                'datasets': [
                    {
                        'label': 'Instruments',
                        'data': data_instruments,
                        'backgroundColor': 'rgba(75, 192, 192, 0.6)',
                        'borderColor': 'rgba(75, 192, 192, 1)',
                        'borderWidth': 1
                    }
                ]
            }

            result = {
                "balances": response,
                "mt_user_id": trading_account["mt_user_id"],
                "user_id": trading_account["user_id"],
                "instrument_data": transformed_data,
                "remark": remark if remark else {},
                "total_buy": total_buy,
                "total_sell": total_sell,
                "trades_data": {
                    "total_trades": total_trades,
                    "total_win": win_count,
                    "total_loss": loss_count,
                    "risk_average": risk_average,
                    "risk_percentage": risk_percentage,
                    "trades_per_day_average": trades_info['trades_per_day_average'],
                    "ratio_win_loss": ratio_win_loss,
                    "total_win_percentage": total_win_percentage,
                    "win_buy_percentage": win_buy_percentage,
                    "win_sell_percentage": win_sell_percentage,
                    "avg_lots": trades_info['avg_lots'],
                    "time_open_trade": trades_info['av_duration'],
                    "commission_trade": trades_info['avg_commission'],

                },
                "max_drawdown_percent": max_drawdown_percent if max_drawdown_percent else 0,
                "balance_info": {
                    "min_balance": min_balance if min_balance else "",
                    "min_balance_day": min_balance_day.strftime('%Y-%m-%d') if min_balance_day else "",
                    "days_to_recover_initial_balance": days_to_recover_initial_balance,
                    "days_since_min_balance": days_since_min_balance
                },
                "symbols_data": {
                    "total_instruments_traded": len(trades_info['symbols_traded'])

                }


            }
            return result

    # WINNERS

    def get_trading_accounts_to_set_by_mt_users_ids(self, mt_users_ids):
        trading_accounts = self.trading_accounts.db.collection.find(
            {"mt_user_id": {"$in": mt_users_ids}})
        return {ta['mt_user_id']: ta for ta in trading_accounts}

    def get_trading_account_metadata_winners(self, account_ids):

        pipeline = [
            {"$match": {"mt_user_id": {"$in": account_ids}, "profit": {"$gt": 0},"status": "active"}},
            {"$group": {"_id": "$mt_user_id", "tradeCount": {"$sum": 1}}},
            {"$sort": {"tradeCount": -1}},
            {"$limit": 30}
        ]

        top_users = list(self.trades.db.collection.aggregate(pipeline))
        trading_accounts = self.get_trading_accounts_to_set_by_mt_users_ids(
            [top_user['_id'] for top_user in top_users]
        )

        result = [self.get_metadata_by_user(trading_accounts[top_user['_id']])
                  for top_user in top_users]

        return result
    # NEWERS

    def get_trading_account_metadata_newers(self, account_ids):

        pipeline = [
                {"$match": {"mt_user_id": {"$in": account_ids}}},
                {"$sort": {"created_at": -1}},
                {"$limit": 20}]

        newer_withdrawals = list(self.withdrawals.db.collection.aggregate(pipeline))
        trading_accounts = self.get_trading_accounts_to_set_by_mt_users_ids(
            [new_withdrawal['_id'] for new_withdrawal in newer_withdrawals]
        )
        result = [self.get_metadata_by_user(trading_accounts[new_withdrawal['_id']]) for new_withdrawal in newer_withdrawals]
        
        return result

    # WITHDRAWALS TOTAL
    def get_trading_account_metadata_withdrawals_total(self, account_ids):
        pipeline = [
            {"$match": {"mt_user_id": {"$in": account_ids},"status": "active"}},
            {'$group': {'_id': '$mt_user_id', 'withdrawalCount': {'$sum': 1}}},
            {'$sort': {'withdrawalCount': -1}},
            {'$limit': 50}]

        top_withdrawal_counts = list(
            self.withdrawals.db.collection.aggregate(pipeline))
        trading_accounts = self.get_trading_accounts_to_set_by_mt_users_ids(
            [top_withdrawal_count['_id']
                for top_withdrawal_count in top_withdrawal_counts]
        )
        result = [self.get_metadata_by_user(
            trading_accounts[top_withdrawal_count['_id']]) for top_withdrawal_count in top_withdrawal_counts]

        return result

    # WITHDRAWALS AMOUNT
    def get_trading_account_metadata_withdrawals_amount(self, account_ids):
        pipeline = [
            {"$match": {"mt_user_id": {"$in": account_ids}, "status": "active"}},
            {'$group': {'_id': '$mt_user_id', 'totalAmount': {'$sum': '$amount'}}},
            {'$sort': {'totalAmount': -1}},
            {'$limit': 20}]

        top_total_amounts = list(
            self.withdrawals.db.collection.aggregate(pipeline))
        trading_accounts = self.get_trading_accounts_to_set_by_mt_users_ids(
            [top_total_amount['_id'] for top_total_amount in top_total_amounts]
        )
        result = [self.get_metadata_by_user(
            trading_accounts[top_total_amount['_id']]) for top_total_amount in top_total_amounts]

        return result

    def get_best_users_by_filter(self):

        try:

            query = {
                "$or": [
                    {"remarks": {"$regex": "k2pa", "$options": "i"}},
                    {"remarks": {"$regex": "k3pn", "$options": "i"}}
                ]
            }

            accounts_mt_user_ids = list(
                self.trading_accounts.db.collection.find(query, {"mt_user_id": 1}))

            account_ids = [account['mt_user_id']
                           for account in accounts_mt_user_ids]

            winners = self.get_trading_account_metadata_winners(account_ids)
            newers = self.get_trading_account_metadata_newers(account_ids)
            withdrawals_total = self.get_trading_account_metadata_withdrawals_total(
                account_ids)
            withdrawals_amount = self.get_trading_account_metadata_withdrawals_amount(
                account_ids)

            # todo optimizar aun mas
            # tengo los mt_user_ids de los 4 filtros
            # consulto las cuentas de trading por mt_user_id
            # obtengo el user_metadata de cada cuenta de trading

            # hacer el split de los 4 filtros

            return {
                "winners": winners,
                "newers": newers,
                "withdrawals_total": withdrawals_total,
                "withdrawals_amount": withdrawals_amount
            }

        except Exception as e:
            print(e)

    def json_serial(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        raise TypeError("Type not serializable")

    @utils.task_log('get_best_users')
    def get_best_users(self):
        if self.chunk_number == 2:
            now_date = datetime.now()
            print('start_task at ' + str(now_date))
            data = self.get_best_users_by_filter()
            winners = data["winners"]
            newers = data["newers"]
            withdrawals_total = data["withdrawals_total"]
            withdrawals_amount = data["withdrawals_amount"]
            winners = json.dumps(winners, default=self.json_serial)
            newers = json.dumps(newers, default=self.json_serial)
            withdrawals_total = json.dumps(
                withdrawals_total, default=self.json_serial)
            withdrawals_amount = json.dumps(
                withdrawals_amount, default=self.json_serial)
            # write 4 files with the data
            path = os.path.join(os.path.dirname(__file__), 'data')
            # if not os.path.exists(path):
            #     os.makedirs(path)
            # with open(os.path.join(path, 'winners.json'), 'w') as outfile:
            #     outfile.write(winners)
            # with open(os.path.join(path, 'newers.json'), 'w') as outfile:
            #     outfile.write(newers)
            # with open(os.path.join(path, 'withdrawals_total.json'), 'w') as outfile:
            #     outfile.write(withdrawals_total)
            # with open(os.path.join(path, 'withdrawals_amount.json'), 'w') as outfile:
            #     outfile.write(withdrawals_amount)

            cliente_redis = redis.StrictRedis(host=os.getenv("REDIS_HOST"), port=int(
                os.getenv("REDIS_PORT")), password=os.getenv("REDIS_PASSWORD"), db=0)
            cliente_redis.set('winners', winners)
            cliente_redis.set('newers', newers)
            cliente_redis.set('withdrawals_total', withdrawals_total)
            cliente_redis.set('withdrawals_amount', withdrawals_amount)
            cliente_redis.close()
            print('end_task at ' + str(datetime.now()) +
                  ' total time: ' + str(datetime.now() - now_date))

    def convert_timestamp_to_date(self,timestamp_str):
        if not timestamp_str:
            return ''

        try:
            # Convierte el string a un entero (timestamp en milisegundos)
            timestamp_ms = int(timestamp_str)
            # Convierte milisegundos a segundos
            timestamp_s = timestamp_ms / 1000.0
            # Convierte a un objeto datetime
            date = datetime.fromtimestamp(timestamp_s)
            return date.strftime('%Y-%m-%d %H:%M:%S')  # Formato de fecha como desees
        except ValueError:
            return 'Invalid timestamp'

    def map_positions_by_broker(self, positions, broker):
        normalized_positions = []

        for position in positions:
            if broker == "metatrader":
                # Mapeo para Metatrader
                print('position',position)
                normalized_position = {
                    "clientId": position['mt_user_id'] if 'mt_user_id' in position else position['login'],
                    "broker": broker,
                    "symbol": position.get('symbol', ''),
                    "volume": position.get('volume', 0),
                    "openPrice": position.get('priceOpen', 0),
                    "currentPrice": position.get('priceCurrent', 0),
                    "profit": position.get('profit', 0),
                    "swap": position.get('swap', 0),
                    "commission": 0,  # Suponiendo que no está disponible en Metatrader
                    "openTime": position.get('dateCreate', ''),
                    "stopLoss": position.get('stopLoss', 0),
                    "takeProfit": position.get('takeProfit', 0)
                }
            elif broker == "matchtrader":
                # Mapeo para Matchtrader
                r_mask = position.get('rMask', {}).get('simple', {})
                normalized_position = {
                    "clientId": position['clientId'],
                    "broker": broker,
                    "symbol": r_mask.get('symbol', ''),
                    "volume": r_mask.get('volume', 0),
                    "openPrice": position.get('openPrice', 0),
                    "currentPrice": position.get('calculationPrice', 0),
                    "profit": position.get('profit', 0),
                    "swap": position.get('swap', 0),
                    "commission": position.get('commission', 0),
                    "openTime": self.convert_timestamp_to_date(position.get('openTime', '')),
                    "stopLoss": r_mask.get('slPrice', 0),
                    "takeProfit": r_mask.get('tpPrice', 0)
                }
            else:
                # Manejar otros brokers o casos desconocidos
                normalized_position = {}

            normalized_positions.append(normalized_position)

        return normalized_positions

    def get_positions(self, filter):
        print('ejecutando get positions')
        all_positions = []
        winners = []
        if filter['winners']:
            winners = utils.do_request(
                                'GET', 
                                'https://back-office-api.cap.cryptofundtrader.com/best_users/newers?page=1&per_page=20&sort_by=-id', 
                                {
                                    'Authorization':'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJmcmVzaCI6ZmFsc2UsImlhdCI6MTcwMDE3ODkxMSwianRpIjoiYzAxYmFmZmEtODc5Zi00ODg2LWE2OGUtZGNjNTc5MmVkOTdiIiwidHlwZSI6ImFjY2VzcyIsInN1YiI6IjY1MGI0YTFhNWE3NGU0NWNmNzEyYjdmMCIsIm5iZiI6MTcwMDE3ODkxMSwiZXhwIjoxNzAwMjY1MzExfQ.xu1fX9mHM9mgHrR_N56FBWRckvDSCWZtgK4hFTM9TMc'
                                }
                            )
            if winners.status_code == 200:
                winners = winners.json()
            if 'content' in winners:
                winners = winners['content']
            
                winners = list( self.trading_accounts.db.collection.find({
                    'mt_user_id': {
                        '$in':  [winner['mt_user_id'] for winner in winners]
                    }
                }))
        elif filter['k3/k2pa']:
            
            winners = list( self.trading_accounts.db.collection.find({
                'remarks': {'$regex': 'k2pa|k3'},
                'status': {'$ne': 'closed'}
            }).sort('created_at', pymongo.ASCENDING).limit(100))
        
        positions = []
        for winner in winners:
            print(winner['mt_user_id'], winner['broker'])
            positions_by_mt_user = self.broker_manager.get_open_positions(winner['mt_user_id'], winner['broker'])
            all_positions  = all_positions +  self.map_positions_by_broker(positions_by_mt_user, winner['broker'])
            positions.append({
                'mt_user_id': winner['mt_user_id'],
                'broker': winner['broker'],
                'positions': positions_by_mt_user
            })
        
         # write file with the data in json
        path = os.path.join(os.path.dirname(__file__), 'data')
        if not os.path.exists(path):
            os.makedirs(path)
        
        with open(os.path.join(path, 'positions10.json'), 'w') as outfile:
            outfile.write(json.dumps(positions, default=self.json_serial, indent=4))

        with open(os.path.join(path, 'all_positions10.json'), 'w') as outfile:
            outfile.write(json.dumps(all_positions, default=self.json_serial, indent=4))

        token = '6882613903:AAGHmxKqNikOpBnaHtkF8DhneGaU8VURFkE'
        channel_id = '-1002014872869'


        utils.do_get_request('https://api.telegram.org/bot' + token + '/sendMessage?chat_id=' + channel_id + '&text=hola')

      
    def get_positions_by_winners(self):
        self.get_positions({'winners': True, 'k3/k2pa': False})
    
    def get_positions_by_k3_k2pa(self):
        self.get_positions({'winners': False, 'k3/k2pa': True})