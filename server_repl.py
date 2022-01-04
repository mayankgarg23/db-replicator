from concurrent import futures

import json
from pymongo import MongoClient
import grpc
import configparser

import db_replicator_pb2
import db_replicator_pb2_grpc

class DB_ReplicatorServicer(db_replicator_pb2_grpc.DB_ReplicatorServicer):

    def Receiver( self, request_iterator, context):
        config = configparser.ConfigParser()
        config.read('mongo_config.ini')

        connection_string = config['mongo']['connection_string']
        
        mongo_client = MongoClient(connection_string)
        
        for record in request_iterator:
            for json_obj in json.loads(record.data):
                kind = json_obj["kind"]
                table_name = json_obj["table"]
                db = mongo_client["college"]
                col = db[table_name]

                if kind == "insert":
                    column_names = json_obj["columnnames"]
                    column_values = json_obj["columnvalues"]
                    data = {}

                    for (column_name, column_value) in zip(column_names, column_values):
                        data[column_name] = column_value
                    
                    col.insert_one(data)
                elif kind == "update":
                    column_names = json_obj["columnnames"]
                    column_values = json_obj["columnvalues"]
                    
                    new_data = {}
                    for (column_name, column_value) in zip(column_names, column_values):
                        new_data[column_name] = column_value
                    
                    old_data = {}
                    key_names = json_obj["oldkeys"]["keynames"]
                    key_values = json_obj["oldkeys"]["keyvalues"]
                    
                    for (key_name, key_value) in zip(key_names, key_values):
                        old_data[key_name] = key_value
                    
                    new_json_data = {}
                    new_json_data["$set"] = new_data
                    
                    col.update_one(old_data, new_json_data)
                elif kind == "delete":
                    key_names = json_obj["oldkeys"]["keynames"]
                    key_values = json_obj["oldkeys"]["keyvalues"]
                    data = {}

                    for (key_name, key_value) in zip(key_names, key_values):
                        data[key_name] = key_value
                    
                    col.delete_one(data)
        
        return db_replicator_pb2.Response(response = "success")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    db_replicator_pb2_grpc.add_DB_ReplicatorServicer_to_server(
        DB_ReplicatorServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("server started")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()

# [{'kind': 'insert', 'schema': 'public', 'table': 'student', 'columnnames': ['id', 'first_name', 'last_name', 'sjsu_id', 'email', 'create_timestamp', 'update_timestamp'], 'columntypes': ['integer', 'text', 'text', 'text', 'text', 'timestamp without time zone', 'timestamp without time zone'], 'columnvalues': [2, 'abc', 'def', '12345', 'abc@gmail.com', '2021-09-17 18:50:36.77023', '2021-09-17 18:50:36.77023']}]
# [{'kind': 'delete', 'schema': 'public', 'table': 'student', 'oldkeys': {'keynames': ['id'], 'keytypes': ['integer'], 'keyvalues': [2]}}]
# [{'kind': 'update', 'schema': 'public', 'table': 'student', 'columnnames': ['id', 'first_name', 'last_name', 'sjsu_id', 'email', 'create_timestamp', 'update_timestamp'], 'columntypes': ['integer', 'text', 'text', 'text', 'text', 'timestamp without time zone', 'timestamp without time zone'], 'columnvalues': [2, 'gkh', 'def', '12345', 'abc@gmail.com', '2021-09-17 19:43:44.767607', '2021-09-17 19:43:44.767607'], 'oldkeys': {'keynames': ['id'], 'keytypes': ['integer'], 'keyvalues': [2]}}]