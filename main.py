import re
import pika, sys, os
import json
from graphData import Device,Resource,NeoConnector
from mongoConnector import MongoConnector


class SelfDescriptionComponent():

    def __init__(self,amqpBroker='localhost',queue='representation',mongoDb='iot'):
        self.amqpBroker = amqpBroker
        self.amqpQueue = queue
        #Get mongodb Client,db and Collection
        self.mongo_client = MongoConnector(dbname=mongoDb)

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
        analisys_data["CREATE"] = {"devices":[],"resources":[],"apps":[]}
        analisys_data["UPDATE"] = {"devices":[],"resources":[],"apps":[]}
        analisys_data["DELETE"] = {"devices":[],"resources":[],"apps":[]}
        #Parsing JSON data.
        data_dic = json.loads(data)
        #print(dict_obj)

        keys_1 = ["resources","devices","apps"]
        keys_2 = ["CREATE","UPDATE","DELETE"]

        #Get last known data
        for first_key in keys_1:
            self.mongo_client.get_collection(first_key)
            for second_key in keys_2:
                self.get_last_representation(data_dic[second_key][first_key],analisys_data[second_key][first_key])

        #Call analysis Process
        self.analysis(analisys_data)


    #Analisis Process
    def analysis(self,data):

        plan_request = {}
        plan_request["CREATE"] = {"devices":[],"resources":[],"apps":[]}
        plan_request["UPDATE"] = {"devices":[],"resources":[],"apps":[]}
        plan_request["DELETE"] = {"devices":[],"resources":[],"apps":[]}
        plan_request["ALERTS"] = {"warnings":[],"Failures":[]}

        keys_1 = ["resources","devices","apps"]
        keys_2 = ["CREATE","UPDATE","DELETE"]

        for first_key in keys_1:
            self.mongo_client.get_collection(first_key)
            for second_key in keys_2:
                if second_key != "DELETE":
                    for item in data[second_key][first_key]:
                        if not item["old"] and item["new"]:
                            #Create Request Plan
                            plan_request["CREATE"][first_key].append(item["new"])
                            #Save Item Representation to MongoDB
                            self.mongo_client.insert_document(item["new"])
                        elif item["old"] and item["new"]:
                            if item["old"] != item["new"]:
                                #Update Request Plan
                                plan_request["UPDATE"][first_key].append(item["new"])
                                #Save update Representation to MongoDB
                                self.mongo_client.update_document(item["new"])
                else:
                    for item in data[second_key][first_key]:
                        if item["old"] and item["new"]:
                            #Create Request Plan
                            plan_request["DELETE"][first_key].append(item["new"])
                            #Save device Representation to MongoDB
                            self.mongo_client.delete_document(item["new"])       
        

            

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
        execute_request["CREATE_NODES"] = {"devices":[],"resources":[],"apps":[]}
        execute_request["UPDATE_NODES"] = {"devices":[],"resources":[],"apps":[]}
        execute_request["DELETE_NODES"] = {"devices":[],"resources":[],"apps":[]}
        execute_request["ALERTS"] = {"warnings":[],"faults":[]}

        #Resources Plan
        for resource in request["CREATE"]["resources"]:
            resource_n = Resource(global_id=resource["global_id"],name=resource["name"],description=resource["description"],type=resource["type"])
            execute_request["CREATE_NODES"]["resources"].append(resource_n)
        
        for resource in request["UPDATE"]["resources"]:
            node = Resource.nodes.get(global_id=resource["global_id"])
            new_node = self.update_resource_node(resource,node)
            execute_request["UPDATE_NODES"]["resources"].append(new_node)
        
        for resource in request["DELETE"]["resources"]:
            node = Resource.nodes.get(global_id=device["global_id"])
            execute_request["DELETE_NODES"]["resources"].append(node)


        #Devices plan                
        for device in request["CREATE"]["devices"]:
            device_n = Device(global_id=device["global_id"],name=device["name"],description=device["description"],create_nodes=device["is_gateway"])
            #[New Device, Device Parent ID, Resources]
            execute_request["CREATE_NODES"]["devices"].append([device_n,device["device_parent"],device["resources"]])

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

        #---------------------------------------------#
        #new resources Nodes
        for resource in plan["CREATE_NODES"]["resources"]:
            connector.saveResource(resource)
        
        #new resources Nodes
        for resource in plan["UPDATE_NODES"]["resources"]:
            connector.updateResource(resource)
        
        #new resources Nodes
        for resource in plan["DELETE_NODES"]["resources"]:
            connector.deleteResource(resource)

        #---------------------------------------------#
        #new device Nodes
        for device in plan["CREATE_NODES"]["devices"]:
            connector.saveDevice(device[0],device[1],device[2])

        #update device Nodes
        for device in plan["UPDATE_NODES"]["devices"]:
            connector.updateDevice(device[0],device[1])
        
        #Delete device Nodes
        for device in plan["DELETE_NODES"]["devices"]:
            connector.deleteDevice(device)


    #Complementary Methods
    """
    Find the data of the last known item data in MongoDb database, 
    depending on the current collection.
    """
    def get_last_representation(self,list,new_list):
        for item in list:
            filter_d = {"global_id":item["global_id"]}
            known_item = self.mongo_client.get_one_document(filter_d)
            item_n = {}
            item_n["new"] = item
            item_n["old"] = known_item                
            new_list.append(item_n)
    
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
    
    def update_resource_node(self,new_data,old_node):
        if new_data["name"] != old_node.name:
            old_node.name = new_data["name"]
        if new_data["description"] != old_node.description: 
            old_node.description = new_data["description"]
        if new_data["resource_type"] != old_node.resource_type: 
            old_node.resource_type = new_data["resource_type"]



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