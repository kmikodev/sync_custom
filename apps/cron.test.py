import os
import json
from common_libraries.classes.database.database import Database
import time
import common_libraries.functions as functions
import utils.cache_data_access as cda
import classes.task.manager as task_manager
from apscheduler.schedulers.background import BackgroundScheduler
from common_libraries.classes.database.database import Database
from common_libraries.classes.database.trading_account import TradingAccount
from common_libraries.classes.database.order import Order
from common_libraries.classes.broker.metatrader import BrokerMetaTrader
from common_libraries.classes.broker.matchtrader import BrokerMatchTrader
from common_libraries.classes.broker.manager import BrokerManager
from datetime import datetime, timedelta, date
import classes.task.manager as task_manager
from bson.objectid import ObjectId
from common_libraries.classes.database.user import User
import common_libraries.utils as utils
from pymongo import MongoClient
from dotenv import load_dotenv
import redis
from telegram import Bot

load_dotenv()

if os.getenv('MONGO_URI'):
    uri = os.getenv('MONGO_URI')
    client = MongoClient(uri)
elif os.getenv('MONGO_USER') and os.getenv('MONGO_PASSWORD'):
    #print ("Mongo Host: " + os.getenv('MONGO_HOST') + " Mongo User: " + os.getenv('MONGO_USER') + " Mongo Password: " + os.getenv('MONGO_PASSWORD') + " Mongo Database: " + os.getenv('MONGO_DATABASE'))
    client = MongoClient(os.getenv('MONGO_HOST'), int(os.getenv('MONGO_PORT')), username=os.getenv('MONGO_USER'), password=os.getenv('MONGO_PASSWORD'))
else:
    client = MongoClient(os.getenv('MONGO_HOST'), 27017)

if os.getenv('MONGO_DATABASE'):
    db = client[os.getenv('MONGO_DATABASE')]
else:
    db = client['cryptofundtrader-dev']


#########BROKERS E INSTANCIAS ####################
bmanager = BrokerManager()
tm = task_manager.TaskManager()

# # tm.get_positions_by_winners()
# tm.get_positions_by_k3_k2pa()

# Use this token to access the HTTP API:
# 6882613903:AAGHmxKqNikOpBnaHtkF8DhneGaU8VURFkE
# Keep your token secure and store it safely, it can be used by anyone to control your bot.


# https://api.telegram.org/bot6882613903:AAGHmxKqNikOpBnaHtkF8DhneGaU8VURFkE/sendMessage?chat_id=-1002014872869&text=Test