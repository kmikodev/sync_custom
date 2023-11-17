from datetime import datetime, timedelta
import json
import classes.task.manager as task_manager
from pymongo import MongoClient, UpdateOne, UpdateMany


tm = task_manager.TaskManager()

# read data_grouped.json

with open('data_grouped.json') as json_file:
    all_data = json.load(json_file)

# Definimos las secuencias posibles
sequences = [
    ["100k1pafee","100k2pafee","100k3pafee"],
    ["100k1pn","100k2pn","100k3pn"],
    ["100k1pnfee","100k2pnfee","100k3pnfee"],
    ["10k1pafee","10k2pafee","10k3pafee"],
    ["10k1pn","10k2pn","10k3pn"],
    ["10k1pnfee","10k2pnfee","10k3pnfee"],
    ["200k1pafee","200k2pafee","200k3pafee"],
    ["200k1pn","200k2pn","200k3pn"],
    ["200k1pnfee","200k2pnfee","200k3pnfee"],
    ["25k1pafee","25k2pafee","25k3pafee"],
    ["25k1pn","25k2pn","25k3pn"],
    ["25k1pnfee","25k2pnfee","25k3pnfee"],
    ["50k1pafee","50k2pafee","50k3pafee"],
    ["50k1pn","50k2pn","50k3pn"],
    ["50k1pnfee","50k2pnfee","50k3pnfee"],
    ["5k1pa","5k2pa","5k3pa"],
    ["5k1pn","5k2pn","5k3pn"],
    ["demo\\100k1pafee","demo\\100k2pafee","demo\\100k3pafee"],
    ["demo\\100k1pn","demo\\100k2pn","demo\\100k3pn"],
    ["demo\\100k1pnfee","demo\\100k2pnfee","demo\\100k3pnfee"],
    ["demo\\10k1pafee","demo\\10k2pafee","demo\\10k3pafee"],
    ["demo\\10k1pn","demo\\10k2pn","demo\\10k3pn"],
    ["demo\\10k1pnfee","demo\\10k2pnfee","demo\\10k3pnfee"],
    ["demo\\200k1pafee","demo\\200k2pafee","demo\\200k3pafee"],
    ["demo\\200k1pn","demo\\200k2pn","demo\\200k3pn"],
    ["demo\\200k1pnfee","demo\\200k2pnfee","demo\\200k3pnfee"],
    ["demo\\25k1pafee","demo\\25k2pafee","demo\\25k3pafee"],
    ["demo\\25k1pn","demo\\25k2pn","demo\\25k3pn"],
    ["demo\\25k1pnfee","demo\\25k2pnfee","demo\\25k3pnfee"],
    ["demo\\50k1pafee","demo\\50k2pafee","demo\\50k3pafee"],
    ["demo\\50k1pn","demo\\50k2pn","demo\\50k3pn"],
    ["demo\\50k1pnfee","demo\\50k2pnfee","demo\\50k3pnfee"],
    ["demo\\5k1pa","demo\\5k2pa","demo\\5k3pa"],
    ["demo\\5k1pn","demo\\5k2pn","demo\\5k3pn"]
]

# Buscar la próxima secuencia basada en el mt_user_id actual y la secuencia a buscar
def find_next_in_sequence(current_date, next_remark, data):
    # Convertir la cadena de fecha y hora de cierre actual a un objeto datetime
    if current_date == '' or current_date == "None":
        return None, None
    
    try:
        close_date_dt = datetime.strptime(current_date.split('.')[0], "%Y-%m-%d %H:%M:%S")
    except:
        if current_date == "":
            close_date_dt = None
        close_date_dt = datetime.strptime(current_date.split('.')[0], "%Y-%m-%d %H:%M:%S.%f")
    
    for account in data["trading_accounts"]:
        if account["mt_user_id"] == '130810':
            print('diff',account)
        # Convertir la cadena de fecha y hora de creación a un objeto datetime
        if account["created_at"] == '':
            continue
        try:
            created_date_dt = datetime.strptime(account["created_at"].split('.')[0], "%Y-%m-%d %H:%M:%S.%f")
        except:
            created_date_dt = datetime.strptime(account["created_at"].split('.')[0], "%Y-%m-%d %H:%M:%S")    
        
        # Calcular la diferencia entre la fecha de creación del siguiente y la fecha de cierre del actual
        delta = created_date_dt - close_date_dt
        
        if account["mt_user_id"] == '130810':
            print('diff',delta)
        # Verificar si la cuenta coincide con la siguiente secuencia y si la diferencia es menor o igual a 10 minutos
        diff = delta.total_seconds()
      
        
        
        if next_remark in account["remarks"] and abs(delta.total_seconds()) <= 3600*30:  # 600 segundos son 10 minutos
            return account["mt_user_id"], account["closed_date"]
    
    return None, None

# funcion que hace el matching de las cuentas
def matching(data):
    users_ids = []

    # Vamos a iterar sobre cada cuenta
    for account in data["trading_accounts"]:
        if account["status"] == 'active':
            continue
        for seq in sequences:
            if (account["mt_user_id"],) in [x[:1] for x in users_ids]:
                continue
            if account["status_reason"] == "closed by win/loss: win" and seq[0] in account["remarks"]:
                
                user_id_x = account["mt_user_id"]
                close_date_x = account["closed_date"]
                # Buscamos la cuenta con seq[1]
                if user_id_x == '130324':
                    print('user_id_x',user_id_x)
                user_id_y, close_date_y = find_next_in_sequence(close_date_x, seq[1], data)
                if user_id_y:
                    # Buscamos la cuenta con seq[2]
                    user_id_z, _ = find_next_in_sequence(close_date_y, seq[2], data)

                    if user_id_z:
                        users_ids.append([user_id_x, user_id_y, user_id_z])
                    else:
                        users_ids.append([user_id_x, user_id_y])
    return users_ids            


result = list(map(matching, all_data))

result = list(filter(  lambda x: len(x) > 1,[inner for sublist in result for inner in sublist]))

total = 0

for item in result:
    if len(item) == 3:
        total += 2
    else:
        total += 1
print('total',total)

elements = []
total2 = 0
for item in result:
        
        for j,element in enumerate(item):
            if j  == len(item) - 1:
                continue
            total2 += 1
            update_  = [
                 {
                    'mt_user_id': element,
                }, {
                    '$set': {
                        'next_mt_user_id': item[j+1],
                        "from" : "match.py"
                    }
                }
            ]
            
            elements.append(UpdateOne({
                'mt_user_id': element,
                'next_mt_user_id': {"$exists": False}
            }, {
                '$set': {
                    'next_mt_user_id': item[j+1],
                    "from" : "match.py"
                }
            }))
            
print ('total2',total2)
print('len(elements)',len(elements))
tm.trading_accounts.db.collection.bulk_write(elements, ordered=False)

# guardar el resultado en un archivo json pero filtrando los elementos del array cuya longitud sea 0
with open('result.json', 'w') as outfile:
    # filtered_data = list(filter(lambda x: len(x) > 0, result))
    json.dump(result, outfile, indent=4)