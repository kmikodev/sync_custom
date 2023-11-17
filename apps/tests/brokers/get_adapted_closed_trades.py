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
trades = broker.get_adapted_metatrader_closed_trades()
for trade in trades:
    print(trade)

