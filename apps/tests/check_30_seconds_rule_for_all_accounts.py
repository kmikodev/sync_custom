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
#tm.update_trades()
tm.check_30_seconds_rule_for_all_accounts()
