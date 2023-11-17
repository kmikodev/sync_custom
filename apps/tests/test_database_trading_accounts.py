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



from classes.database.trading_account import TradingAccount

trading_account = TradingAccount()
print(trading_account.get_all_by_user_id(1))
