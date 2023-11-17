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

from classes.broker.metatrader import BrokerMetaTrader

broker = BrokerMetaTrader()
new_data = {
    'group': 'demo\\25k1pn',
}
result = broker.edit_account(2342, new_data)
if result:
    print('account edited')
else:
    print('account not edited')