import re
import pika, sys, os
import json
from graphData import Device,NeoConnector
from mongoConnector import MongoConnector


class SelfDescriptionComponent():

    def __init__(self,amqpBroker='localhost',queue='representation',mongoDb='iot',mongoCollection='components_data'):
        self.amqpBroker = amqpBroker
        self.amqpQueue = queue
        #Get mongodb Client,db and Collection
        self.mongo_client = MongoConnector(dbname=mongoDb)
        self.mongo_client.create_or_set_collection(mongoCollection)

    #Init AMQP conection data process
    def init_mapek(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.amqpBroker))
        channel = connection.channel()
        channel.queue_declare(queue=self.amqpQueue)

        def callback(ch, method, properties, message):
            #Call monitor process
            self.monitor(message)
        
        channel.basic_consume(queue=self.amqpQueue, on_message_callback=callback, auto_ack=True)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()

    #Monitor Process
    def monitor(self,data):
        analisys_data = {}
        analisys_data["devices"] = []
        analisys_data["resources"] = []
        analisys_data["properties"] = []

        #Parsing JSON data.
        dict_obj = json.loads(data)
        print(dict_obj)
        """
        
        #Create request
        if dict_obj["opt_type"]=="CREATE":
            analisys_data["opt_type"] = "CREATE"
            for device in dict_obj["devices"]:
                filter_d = {"global_id":device["global_id"]}
                document = self.mongo_client.get_one_document(filter_d)
                device_t = {}
                device_t["new"] = device
                device_t["old"] = document
                analisys_data["devices"].append(device_t)
        

        #Call analysis Process
        self.analysis(analisys_data)
        """


    #Analisis Process
    def analysis(self,data):

        plan_request = {}
        plan_request["CREATE"] = {}
        plan_request["CREATE"]["devices"] = []


        if data["opt_type"]=="CREATE":
            if data["devices"]:
                for device in data["devices"]:
                    if not device["old"] and device["new"]:
                        #Create Request To Plan
                        plan_request["CREATE"]["devices"].append(device["new"])
                        #Save device Representation to MongoDB
                        self.mongo_client.insert_document(device["new"])

        """
        if dict_obj["device"]:
            request["new"] = {}
            request["new"]["devices"] = []
            request["new"]["devices"].append(dict_obj["device"])"""
            
        self.plan(plan_request)

    #Plan Process
    def plan(self,request):   
        execute_request = {}
        execute_request["create_nodes"] = {}
        execute_request["create_nodes"]["devices"] = []
        execute_request["alerts"] = []
                
        if request["CREATE"]:
            if request["CREATE"]["devices"]:
                for device in request["CREATE"]["devices"]:
                    device_n = Device(global_id=device["global_id"],name=device["name"],description=device["description"],create_nodes=device["is_gateway"])
                    execute_request["create_nodes"]["devices"].append([device_n,device["device_parent"]])           
        
        self.execute(execute_request)

    #Execute Process
    def execute(self,plan):
        connector = NeoConnector()
        if len(plan["create_nodes"]["devices"])!=0:
            for device in plan["create_nodes"]["devices"]:
                connector.saveDevice(device[0],device[1])        

#Principal Process
if __name__ == '__main__':
    try:
        auto_component = SelfDescriptionComponent()
        auto_component.init_mapek()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)