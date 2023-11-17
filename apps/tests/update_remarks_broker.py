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

import functions as functions

#Update demo, pass False
#functions.update_broker_trading_account_remarks('215127', '25k1pn')
functions.update_broker_trading_account_remarks('250297', '100k1pn')
