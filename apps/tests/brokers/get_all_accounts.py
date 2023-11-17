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

#add parents
parent = os.path.dirname(parent)
sys.path.append(parent)

#add parents
parent = os.path.dirname(parent)
sys.path.append(parent)

from classes.broker.manager import BrokerManager

broker = BrokerManager()
accounts = broker.get_all_accounts()

matchtrader_accounts_count = 0
metatrader_accounts_count = 0

for account in accounts:
    if account['broker'] == 'matchtrader':
        matchtrader_accounts_count += 1
    elif account['broker'] == 'metatrader':
        metatrader_accounts_count += 1

print('matchtrader_accounts_found: ' + str(matchtrader_accounts_count))
print('metatrader_accounts_found: ' + str(metatrader_accounts_count))
