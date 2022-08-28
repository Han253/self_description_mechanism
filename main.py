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
        analisys_data["CREATE"] = {"devices":[]}
        analisys_data["UPDATE"] = {"devices":[]}
        analisys_data["DELETE"] = {"devices":[]}
        #Parsing JSON data.
        data_dic = json.loads(data)
        #print(dict_obj)
        
        
        #Create device request
        if len(data_dic["CREATE"]["devices"]) !=0:            
            self.find_device_old_representation(data_dic["CREATE"]["devices"],analisys_data["CREATE"]["devices"])
        
        #Update device request
        if len(data_dic["UPDATE"]["devices"]) !=0:            
            self.find_device_old_representation(data_dic["UPDATE"]["devices"],analisys_data["UPDATE"]["devices"])            
        
        #Delete device request
        if len(data_dic["DELETE"]["devices"]) !=0:            
            self.find_device_old_representation(data_dic["DELETE"]["devices"],analisys_data["DELETE"]["devices"])          
        
        #Call analysis Process
        self.analysis(analisys_data)


    #Analisis Process
    def analysis(self,data):

        plan_request = {}
        plan_request["CREATE"] = {"devices":[]}
        plan_request["UPDATE"] = {"devices":[]}
        plan_request["DELETE"] = {"devices":[]}
        plan_request["ALERTS"] = {"warnings":[],"Failures":[]}

        for device in data["CREATE"]["devices"]:
            if not device["old"] and device["new"]:
                #Create Request Plan
                plan_request["CREATE"]["devices"].append(device["new"])
                #Save device Representation to MongoDB
                self.mongo_client.insert_document(device["new"])
            elif device["old"] and device["new"]:
                if device["old"] != device["new"]:
                    #Update Request Plan
                    plan_request["UPDATE"]["devices"].append(device["new"])
                    #Save update Representation to MongoDB
                    self.mongo_client.update_document(device["new"])
        
        for device in data["UPDATE"]["devices"]:
            if not device["old"] and device["new"]:
                #Create Request Plan
                plan_request["CREATE"]["devices"].append(device["new"])
                #Save device Representation to MongoDB
                self.mongo_client.insert_document(device["new"])
            elif device["old"] and device["new"]:
                if device["old"] != device["new"]:
                    #Update Request Plan
                    plan_request["UPDATE"]["devices"].append(device["new"])
                    #Save update Representation to MongoDB
                    self.mongo_client.update_document(device["new"])
        
        for device in data["DELETE"]["devices"]:
            if device["old"] and device["new"]:
                #Create Request Plan
                plan_request["DELETE"]["devices"].append(device["new"])
                #Save device Representation to MongoDB
                self.mongo_client.delete_document(device["new"])

            

        #Rules Evaluation
        """total_d = 3
        if len(self.mongo_client.get_all()) > total_d:
            plan_request["ALERTS"]["warnings"].append("The number of devices is very hight")
        else:
            plan_request["ALERTS"]["warnings"].append("The number of devices is very low")"""

        self.plan(plan_request)
           
        

    #Plan Process
    def plan(self,request):   
        execute_request = {}
        execute_request["CREATE_NODES"] = {"devices":[]}
        execute_request["UPDATE_NODES"] = {"devices":[]}
        execute_request["DELETE_NODES"] = {"devices":[]}
        execute_request["ALERTS"] = {"warnings":[],"faults":[]}
                
        for device in request["CREATE"]["devices"]:
            device_n = Device(global_id=device["global_id"],name=device["name"],description=device["description"],create_nodes=device["is_gateway"])
            #[New Device, Device Parent ID]
            execute_request["CREATE_NODES"]["devices"].append([device_n,device["device_parent"]])

        for device in request["UPDATE"]["devices"]:
            node = Device.nodes.get(global_id=device["global_id"])
            new_node,device_parent = self.update_device_node(device,node)
            execute_request["UPDATE_NODES"]["devices"].append([new_node,device_parent])
        
        for device in request["DELETE"]["devices"]:
            node = Device.nodes.get(global_id=device["global_id"])
            execute_request["DELETE_NODES"]["devices"].append(node)
        
        self.execute(execute_request)

    #Execute Process
    def execute(self,plan):
        connector = NeoConnector()
        #new device Nodes
        for device in plan["CREATE_NODES"]["devices"]:
            connector.saveDevice(device[0],device[1])

        #update device Nodes
        for device in plan["UPDATE_NODES"]["devices"]:
            connector.updateDevice(device[0],device[1])
        
        #Delete device Nodes
        for device in plan["DELETE_NODES"]["devices"]:
            connector.deleteDevice(device)


    #Complementary Methods
    def find_device_old_representation(self,old_list,new_list):
        if(len(old_list)!=0):
            for device in old_list:
                filter_d = {"global_id":device["global_id"]}
                document = self.mongo_client.get_one_document(filter_d)
                device_n = {}
                device_n["new"] = device
                device_n["old"] = document                
                new_list.append(device_n)
    
    def update_device_node(self,new_data,old_node):
        device_parent = None
        #Update Properties
        if new_data["name"] != old_node.name: 
            old_node.name = new_data["name"]
        if new_data["description"] != old_node.description: 
            old_node.description = new_data["description"]
        if new_data["is_gateway"] != old_node.is_gateway:
            old_node.is_gateway = new_data["is_gateway"]
        for parent in old_node.device_parent:
            if new_data["device_parent"] != parent.global_id:
                device_parent = new_data["device_parent"]        
        return old_node, device_parent



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