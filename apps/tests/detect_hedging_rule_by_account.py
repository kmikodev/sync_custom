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
#tm.update_users_collection()
#tm.update_trades_collection()

#Match
#tm.detect_hedging_rule_by_account(313845)#no salta y deberia

# #Meta
#tm.detect_hedging_rule_by_account(125171)#salta y no deberia
tm.detect_hedging_rule_by_account(125654)#deberia y no salta
