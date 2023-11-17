import json
import os
import sys
from bson.objectid import ObjectId

import classes.task.manager as task_manager

# read file execution.json

# execution = json.load(open('execution.json'))

# result = []

# for ex in execution['execution_entity']:
#     ex['data'] = json.loads(ex['data'])
#     ex['workflowData'] = json.loads(ex['workflowData'])
#     result.append(ex)
    
# # write to file execution.json
# with open('execution2.json', 'w') as outfile:
#     json.dump(result, outfile, indent=4)
    
# execution = json.load(open('execution2.json'))
# result = []
# for data in execution:
#     keys = next(item for item in data['data'] if isinstance(item, dict) and item.get('environment') and item.get('user_id') and item.get('broker') and item.get('mt_user_id') and item.get('email') and item.get('firstname') and item.get('lastname') and item.get('operation') and item.get('status_reason') and item.get('old_remarks') and item.get('new_remarks') and item.get('daily_drawdown') and item.get('max_drawdown') and item.get('equity') and item.get('certificate_url') and item.get('phase'))
#     if data['data'][int(keys['operation'])] == 'win':
#         resultData = {
#             "id": data['id'],
#             "startedAt": data['startedAt'],
#             "environment": data['data'][int(keys['environment'])],
#                 "user_id": data['data'][int(keys['user_id'])],
#                 "broker": data['data'][int(keys['broker'])],
#                 "mt_user_id": data['data'][int(keys['mt_user_id'])],
#                 "email": data['data'][int(keys['email'])],
#                 "firstname": data['data'][int(keys['firstname'])],
#                 "lastname": data['data'][int(keys['lastname'])],
#                 "operation": data['data'][int(keys['operation'])],
#                 "status_reason": data['data'][int(keys['status_reason'])],
#                 "old_remarks": data['data'][int(keys['old_remarks'])],
#                 "new_remarks": data['data'][int(keys['new_remarks'])],
#                 "daily_drawdown": data['data'][int(keys['daily_drawdown'])],
#                 "max_drawdown": data['data'][int(keys['max_drawdown'])],
#                 "equity": data['data'][int(keys['equity'])],
#                 "certificate_url": data['data'][int(keys['certificate_url'])],
#                 "phase": data['data'][int(keys['phase'])],
#         }
        
#         result.append(resultData)
    

# with open('execution4.json', 'w') as outfile:
#     json.dump(result, outfile, indent=4)

    
tm = task_manager.TaskManager()


# Buscar documentos donde 'created_at' no existe
accounts_without_created_at = tm.trading_accounts.db.collection.find({'created_at': {'$exists': False}})

for account in accounts_without_created_at:
    # Extraer el tiempo de creación del campo '_id'
    timestamp = ObjectId(account['_id']).generation_time

    # Actualizar el campo 'created_at' del documento
    tm.trading_accounts.db.collection.update_one({'_id': account['_id']}, {'$set': {'created_at': timestamp}})

# all_data = json.load(open('dates.json'))

# for data in all_data:
#     if data.get('created_at'):
#         tm.trading_accounts.db.collection.update_one({'mt_user_id': data['mt_user_id']}, {'$set': {'created_at': data['created_at']}})