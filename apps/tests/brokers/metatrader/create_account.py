import os
import sys
import random
import string

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
def generate_random_password(number_of_chars = 12):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(number_of_chars))

password = generate_random_password()

data = {
    'email': 'mcolomer7@killia.com',
    'remarks': 'demo\\10k1pn',

    'fullname': 'Miquel Colomer',
    'firstname': 'Miquel',
    'lastname': 'Colomer',
    'address': 'Carrer de la Llibertat, 47, 08911 Badalona, Barcelona',
    'country': 'Spain',
    'phone': '+34 933 84 00 00',
    'comment': 'test',
    'lead_source': 'test',
    'lead_campaign': 'test',
    'password': password,
    'balance': 10000
}
print ("password: " + password)
account = broker.create_account(data)
print(account)

