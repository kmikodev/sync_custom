import os
import sys

# getting the name of the directory
# where the this file is present.
current = os.path.dirname(os.path.realpath(__file__))
 
# Getting the parent directory name
# where the current directory is present.
parent = os.path.dirname(current)
 
# adding the parent directory to
# the sys.path.
sys.path.append(parent)


import classes.task.manager as task_manager

tm = task_manager.TaskManager()
tm.close_account_by_email('pruebahedging1@cryptofundtrader.com')
#tm.close_account_by_email('pruebahedging2@cryptofuntrader.com')
