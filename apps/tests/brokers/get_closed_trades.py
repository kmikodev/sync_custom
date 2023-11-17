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
trades = broker.get_closed_trades()

matchtrader_trades_count = 0
metatrader_trades_count = 0

for trade in trades:
    if trade['broker'] == 'matchtrader':
        matchtrader_trades_count += 1
    elif trade['broker'] == 'metatrader':
        metatrader_trades_count += 1

print('matchtrader_trades_found: ' + str(matchtrader_trades_count))
print('metatrader_trades_found: ' + str(metatrader_trades_count))
