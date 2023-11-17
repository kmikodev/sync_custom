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

# tm.update_users_collection()
#tm.update_trades_collection()
tm.create_30seconds_positions_and_detect(302565)
#tm.create_30seconds_positions_and_detect(302568)
#tm.create_30seconds_positions_and_detect(302570)