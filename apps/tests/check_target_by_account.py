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
#tm.check_target_and_update_by_account(287096)#matchtrader
#tm.check_target_and_update_by_account(287216)#matchtrader
#tm.check_target_and_update_by_account(287100)#matchtrader win
#tm.check_target_and_update_by_account(287096)#matchtrader win
#tm.check_target_and_update_by_account(289003)#matchtrader win
#tm.check_target_and_update_by_account(288988)#matchtrader win

#tm.check_target_and_update_by_account(3087)#metatrader
#tm.check_target_and_update_by_account(3088)#metatrader
#tm.check_target_and_update_by_account(3089)#metatrader
#tm.check_target_and_update_by_account(3090)#metatrader
#tm.check_target_and_update_by_account(3091)#metatrader

#tm.check_target_and_update_by_account(287069)#matchtrader
#tm.check_target_and_update_by_account(3078)#metatrader
tm.check_target_and_update_by_account(283299)