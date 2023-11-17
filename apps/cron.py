import os
import time
import common_libraries.functions as functions
import classes.task.manager as task_manager
from apscheduler.schedulers.background import BackgroundScheduler
os.environ['TZ'] = os.getenv('TZ', 'UTC')

scheduler = BackgroundScheduler()
scheduler.configure(timezone='UTC') 


print (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

tm = task_manager.TaskManager()

scheduler.add_job(tm.get_positions_by_winners, 'cron', second='30') 
scheduler.add_job(tm.get_positions_by_k3_k2pa, 'cron', second='30') 


# Use this token to access the HTTP API:
# 6882613903:AAGHmxKqNikOpBnaHtkF8DhneGaU8VURFkE
# Keep your token secure and store it safely, it can be used by anyone to control your bot.