**DB Name**

college

**Table/Collection name**

students

- Set these three parameters in postgresql.conf file -
   - wal_level = logical
   - max_replication_slots = 10
   - max_wal_senders = 1

After changing these parameters, a restart is needed.

- Add a replication connection rule at pg_hba.conf
   - local   replication     all                                     trust

## Install all the requirements

python3 -m pip install -r requirements.txt

**Change the configurations in postgres_config.ini file to connect with Postgres**

**Change the configurations (connection string) in mongo_config.ini file to connect with MongoDB**

## How to compile IDL

```sh
python3 -m grpc_tools.protoc -Iprotos --python_out=. --grpc_python_out=. protos/db_replicator.proto
```

## Run Server

**Change the configurations (connection string) in mongo_config.ini file to connect with MongoDB**

```sh
python3 server_repl.py
```

## Run Client

**Change the configurations in postgres_config.ini file to connect with Postgres**

```sh
python3 client_repl.py
```
