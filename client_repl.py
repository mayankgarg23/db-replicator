from grpc_requests import StubClient
from db_replicator_pb2 import _DB_REPLICATOR

import db_replicator_pb2

import json
import psycopg2
from psycopg2.extras import LogicalReplicationConnection
import configparser

def _iterator (records):
    for record in records:
            yield db_replicator_pb2.Sender(data=json.dumps(record))

def run(payload):
    client = StubClient("localhost:50051", [_DB_REPLICATOR])
    db_replicator = client.service("db_replicator.DB_Replicator")
    
    records = []
    while True:
        end = payload.rfind("}")
        start = payload.rfind('"change"')
        start = payload.rfind("{", 0, start)
        temp = payload[start:end + 1]
        payload = payload[0:start]
        json_ob = json.loads(temp)
        records.insert(0, json_ob["change"])

        if(len(payload)==0):
            break
    it = _iterator(records)
    result = db_replicator.Receiver(it)

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('postgres_config.ini')
    my_connection  = psycopg2.connect(dbname=config['postgres']['db'], host=config['postgres']['host'], port=config['postgres']['port'], user=config['postgres']['user'], password=config['postgres']['pass'], connection_factory = LogicalReplicationConnection)
    cur = my_connection.cursor()

    try:
        cur.drop_replication_slot('wal2json_test_slot')
    except:
        cur.create_replication_slot('wal2json_test_slot', output_plugin = 'wal2json')
        cur.start_replication(slot_name = 'wal2json_test_slot', options = {'pretty-print' : 1}, decode= True)
    else:
        cur.create_replication_slot('wal2json_test_slot', output_plugin = 'wal2json')
        cur.start_replication(slot_name = 'wal2json_test_slot', options = {'pretty-print' : 1}, decode= True)

    def consume(msg):
        run(msg.payload)

    cur.consume_stream(consume)
    # run()