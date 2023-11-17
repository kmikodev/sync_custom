import os
import json
import time
import common_libraries.functions as functions
import classes.task.manager as task_manager
from apscheduler.schedulers.background import BackgroundScheduler
from common_libraries.classes.database.database import Database
from common_libraries.classes.database.trading_account import TradingAccount
from common_libraries.classes.database.order import Order
from pymongo import MongoClient, UpdateOne, UpdateMany
from pymongo.errors import BulkWriteError
from kombu import Connection, Exchange, Queue, Producer
from common_libraries.classes.database.order import Order
from common_libraries.classes.database.trade import Trade
from common_libraries.classes.database.withdrawal import Withdrawal
from pymongo import MongoClient
from datetime import datetime
import redis
from bson import ObjectId


'''timestamp_ms = 1696310402000

# Convertir milisegundos a segundos
timestamp_s = timestamp_ms'''


def json_serial(obj):
    if isinstance(obj, ObjectId):
        return str(obj)  # Convierte ObjectId a string
    raise TypeError("Type not serializable")


tm = task_manager.TaskManager()
#data = tm.get_best_users_by_filter()
#result_json = json.dumps(data, default=json_serial)
result_json="hola"
cliente_redis = redis.StrictRedis(host=os.getenv("REDIS_HOST"), port=int(os.getenv("REDIS_PORT")), password=os.getenv("REDIS_PASSWORD"), db=0)
cliente_redis.set('result_data', result_json)


