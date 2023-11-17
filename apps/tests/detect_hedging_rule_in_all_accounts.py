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

# import functions as functions
# functions.check_targets_and_update()

import classes.task.manager as task_manager
tm = task_manager.TaskManager()
#tm.update_users_collection()
#tm.update_trades_collection()
detected_accounts = tm.detect_hedging_rule_in_all_accounts()
for detected_account in detected_accounts:
    print(detected_account)
    print("\n")