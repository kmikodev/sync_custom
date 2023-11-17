# How to setup a new environment
Create a new .venv (detected by visual studio)
```
python3 -m venv .venv
source .venv/bin/activate
```

# Install new package for python
```
pip install pymongo
```

# Save current packages
```
pip freeze > requirements.txt
``` 

# How to run it
```
python apps/cron.py
```

# How to run mongo locally
```
docker run -d --name mongo \
	-e MONGO_INITDB_ROOT_USERNAME=mongoadmin \
	-e MONGO_INITDB_ROOT_PASSWORD=secret \
    -v /data/mongo:/data/db \
	mongo

docker run -d --name mongo \
    -p 27017:27107 \
    -v /data/mongo:/data/db \
	mongo

docker run -it --rm mongo \
	mongosh --host 0.0.0.0 \
		-u mongoadmin \
		-p secret \
		--authenticationDatabase admin \
        admin

docker run -it --rm mongo -v /home/mcolomer/uproc/scripts:/scripts mongosh -h 0.0.0.0 -f /scripts/purge_old_documents.js
docker run -it --rm mongo -v /root/scripts:/scripts mongosh --host mongo.uproc.io -u mongoadmin -p secret --authenticationDatabase admin -f /scripts/purge_old_documents.js
```
# How to create mongo user

```
use cryptofundtrader
db.createUser(
   {
     user: "cryptofundtrader",
     pwd: 'k1ll142023',
     roles: [ "readWrite", "dbAdmin" ]
   }
)
```

# How to deploy
```
caprover login
# Add captain.cr.cryptofundtrader.com
# Use secret
# Add alias: cft
caprover deploy
# Choose caprover instance: cft
# Choose caprover app: cft-api (prod), cft-api-dev (dev)
# Choose caprover branch: dev or master (prod)
# Ignore files: Y
```

# Tasks

All tasks are detailed at https://www.notion.so/Tasks-bee56b18e4194b9a91dd786c5cd0ed51

# How to synchronize data
```
python3 apps/sync_real.py

cd database
mongodump -d cryptofundtrader

#user dump folder: cryptofundtrader

#prod
mongorestore --host captain.cr.cryptofundtrader.com --port 1234 --db cryptofundtrader -u root -p PASSWORD --authenticationDatabase=admin dump/cryptofundtrader --drop
#dev
mongorestore --host captain.cr.cryptofundtrader.com --port 1234 --db cryptofundtrader-dev -u root -p PASSWORD --authenticationDatabase=admin dump/cryptofundtrader --drop
```
Remove Port Mapping after operation
```

```
# How to restore dump
```
mongodump --host captain.cr.cryptofundtrader.com --port 1234 --db cryptofundtrader -u root -p PASSWORD --authenticationDatabase=admin

mongorestore --drop
```

# How to import real data

If you have a local mongo server, try next to import data from database folder
```
mongoimport --db cryptofundtrader --collection users --file users.json --jsonArray --drop
mongoimport --db cryptofundtrader --collection trades --file trades.json --jsonArray --drop
mongoimport --db cryptofundtrader --collection trading_accounts --file trading_accounts.json --jsonArray --drop
```
If you use CapRover, go to https://captain.cr.cryptofundtrader.com/#/apps/details/mongo, and check if there is any Port Mapping.
If not available, create one with: Server Port = 1234 > Container Port = 27107
Later, try next to import sample data
```
#prod
mongoimport --host captain.cr.cryptofundtrader.com --port 1234 --db cryptofundtrader --collection users --file users.json --jsonArray  --drop -u root -p PASSWORD --authenticationDatabase=admin
mongoimport --host captain.cr.cryptofundtrader.com --port 1234 --db cryptofundtrader --collection trades --file trades.json --jsonArray  --drop -u root -p PASSWORD --authenticationDatabase=admin
mongoimport --host captain.cr.cryptofundtrader.com --port 1234 --db cryptofundtrader --collection trading_accounts --file trading_accounts.json --jsonArray  --drop -u root -p PASSWORD --authenticationDatabase=admin

#dev
mongoimport --host captain.cr.cryptofundtrader.com --port 1234 --db cryptofundtrader-dev --collection users --file users.json --jsonArray  --drop -u root -p PASSWORD --authenticationDatabase=admin
mongoimport --host captain.cr.cryptofundtrader.com --port 1234 --db cryptofundtrader-dev --collection trades --file trades.json --jsonArray  --drop -u root -p PASSWORD --authenticationDatabase=admin
mongoimport --host captain.cr.cryptofundtrader.com --port 1234 --db cryptofundtrader-dev --collection trading_accounts --file trading_accounts.json --jsonArray  --drop -u root -p PASSWORD --authenticationDatabase=admin 
```
Remove Port Mapping after operation

# Modify documents
```
db.remarks.updateMany({'30_seconds_rule': {$exists: false}}, {$set: {'30_seconds_rule': true}})
db.remarks.updateMany({'sku': {$exists: false}}, {$set: {'sku': 1}})

db.remarks.updateMany({'30seconds_rule': {$exists: true}}, {$unset: {'30seconds_rule': null}})
db.remarks.updateMany({'sku': {$exists: true}}, {$unset: {'sku': null}})

db.remarks.updateMany({'sku': '6'}, {$set: {'sku': 6}});
db.remarks.updateMany({'sku': '7'}, {$set: {'sku': 7}});
db.remarks.updateMany({'sku': '8'}, {$set: {'sku': 8}});
db.remarks.updateMany({'sku': '9'}, {$set: {'sku': 9}});
db.remarks.updateMany({'sku': '10'}, {$set: {'sku': 10}});
db.remarks.updateMany({'sku': '11'}, {$set: {'sku': 11}});
db.remarks.updateMany({'sku': '12'}, {$set: {'sku': 12}});
db.remarks.updateMany({'sku': '13'}, {$set: {'sku': 13}});
db.remarks.updateMany({'sku': '14'}, {$set: {'sku': 14}});
db.remarks.updateMany({'sku': '15'}, {$set: {'sku': 15}});
db.remarks.updateMany({'sku': '16'}, {$set: {'sku': 16}});
db.remarks.updateMany({'sku': '17'}, {$set: {'sku': 17}});
db.remarks.updateMany({'sku': '18'}, {$set: {'sku': 18}});
db.remarks.updateMany({'sku': '19'}, {$set: {'sku': 19}});
db.remarks.updateMany({'sku': '20'}, {$set: {'sku': 20}});


db.remarks.updateMany({'sku': {$in: [6,7,8,9,10,16,17,18,19,20]}}, {$set: {'30_seconds_rule': false}})

db.remarks.updateMany({'sku': {$gte: 11}}, {$set: {'broker': 'metatrader'}})

```