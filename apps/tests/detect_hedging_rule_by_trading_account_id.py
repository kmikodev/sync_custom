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

#Match
#tm.detect_hedging_rule_by_trading_account_id(303491)
#tm.detect_hedging_rule_by_trading_account_id(303490)
#No hedging rule detected
#tm.detect_hedging_rule_by_trading_account_id(303487)

#Meta
#tm.detect_hedging_rule_by_trading_account_id(125252)
tm.detect_hedging_rule_by_trading_account_id(125251)
tm.detect_hedging_rule_by_trading_account_id(125250)