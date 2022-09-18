import re
import pika, sys, os
import json
from graphData import Device,Resource,App,NeoConnector, Service
from mongoConnector import MongoConnector
import typer
import logging
from datetime import datetime

#Typer App CLI Helper
app = typer.Typer()

#Log File
logger = logging.getLogger('mape_k_loger')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('register.log')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

class SelfDescriptionComponent():

    ITEMS_TYPES = ["property","resource","device","app","service"]
    RELATIONS_TYPES = ["device_resource","app_device","app_service","service_resource"]
    QUEUE_TYPES = ["create","update","delete"]

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
            #print(message)
        
        channel.basic_consume(queue=self.amqpQueue, on_message_callback=callback, auto_ack=True)

        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()

    #Monitor Process
    def monitor(self,data):
        analysis_data = {}
        #Create data analysis structure
        for queue in self.QUEUE_TYPES:
            analysis_data[queue] = {}
            for item in self.ITEMS_TYPES:
                analysis_data[queue][item] = []
            for relation in self.RELATIONS_TYPES:
                analysis_data[queue][relation] = []     
        
        #Parsing JSON data.
        data_dic = json.loads(data)

        #Obtain last known data
        for queue in self.QUEUE_TYPES:
            for item in self.ITEMS_TYPES:
                if item in data_dic[queue]:              
                    self.mongo_client.get_collection(item)
                    self.get_last_representation(data_dic[queue][item],analysis_data[queue][item])
            for relation in self.RELATIONS_TYPES:
                if relation in data_dic[queue]:
                    for dat in data_dic[queue][relation]:
                        analysis_data[queue][relation].append(dat)
         
        #Call analysis Process
        self.analysis(analysis_data)


    #Analisis Process
    def analysis(self,data):

        plan_request = {}
        #Create data analysis structure
        for queue in self.QUEUE_TYPES:
            plan_request[queue] = {}
            for item in self.ITEMS_TYPES:
                plan_request[queue][item] = []
            for relation in self.RELATIONS_TYPES:
                plan_request[queue][relation] = []
        plan_request["ALERTS"] = {"warnings":[],"Failures":[],"logs":[]}

        for queue in self.QUEUE_TYPES:
            for item_type in self.ITEMS_TYPES:
                self.mongo_client.get_collection(item_type)
                if queue != "delete":                                                                
                    for item in data[queue][item_type]:
                        if not item["old"] and item["new"]:
                            #Create Request Plan
                            if item_type != "property":
                                plan_request["create"][item_type].append(item["new"])
                            else:
                                str_log = "CREATE PROPERTY G_ID: "+str(item["new"]["global_id"])
                                plan_request["ALERTS"]["logs"].append(str_log)
                                str_log+= " |"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                logger.info(str_log)
                                
                            #Save Item Representation to MongoDB
                            self.mongo_client.insert_document(item["new"])
                        elif item["old"] and item["new"]:
                            if item["old"] != item["new"]:
                                #Update Request Plan
                                if item_type != "property":
                                    plan_request["update"][item_type].append(item["new"])
                                #Save update Representation to MongoDB
                                self.mongo_client.update_document(item["new"])
                else:
                    for item in data[queue][item_type]:
                        if item["old"] and item["new"]:
                            #Delete Request Plan
                            if item_type != "property":
                                plan_request["delete"][item_type].append(item["new"])
                                
                            else:
                                str_log = "DELETE PROPERTY G_ID: "+str(item["new"]["global_id"])
                                plan_request["ALERTS"]["logs"].append(str_log)                                
                                str_log += " |"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                logger.info(str_log)
                            #Save device Representation to MongoDB
                            self.mongo_client.delete_document(item["new"])
            
            
            self.mongo_client.get_collection("relations")
            for relation_type in self.RELATIONS_TYPES:
                for relation in data[queue][relation_type]:
                    if queue == "create":
                        if relation_type == "device_resource":
                            filter_d = {"global_id":relation["device_id"],"type":"device_resource"}
                            known_item = self.mongo_client.get_one_document(filter_d)
                            if known_item:
                                if relation["resource_id"] not in known_item["resources"]:
                                    known_item["resources"].append(relation["resource_id"])
                                    self.mongo_client.update_document_relation(known_item,"device_resource")
                                    plan_request[queue][relation_type].append(relation)
                            else:
                                relation_d = {"type":"device_resource","global_id":relation["device_id"],"resources":[relation["resource_id"]]}
                                self.mongo_client.insert_document(relation_d)
                                plan_request[queue][relation_type].append(relation)
                        if relation_type == "app_device":
                            filter_d = {"global_id":relation["app_id"],"type":"app_device"}
                            known_item = self.mongo_client.get_one_document(filter_d)
                            if known_item:
                                if relation["device_id"] not in known_item["devices"]:
                                    known_item["devices"].append(relation["device_id"])
                                    self.mongo_client.update_document_relation(known_item,"app_device")
                                    plan_request[queue][relation_type].append(relation)
                            else:
                                relation_d = {"type":"app_device","global_id":relation["app_id"],"devices":[relation["device_id"]]}
                                self.mongo_client.insert_document(relation_d)
                                plan_request[queue][relation_type].append(relation)
                        else:
                            plan_request[queue][relation_type].append(relation)

                    if queue == "delete":
                        if relation_type == "device_resource":
                            filter_d = {"global_id":relation["device_id"],"type":"device_resource"}
                            known_item = self.mongo_client.get_one_document(filter_d)
                            if known_item:
                                if relation["resource_id"] in known_item["resources"]:
                                    known_item["resources"].remove(relation["resource_id"])
                                    self.mongo_client.update_document_relation(known_item,"device_resource")
                                    plan_request[queue][relation_type].append(relation)                            
                        if relation_type == "app_device":
                            filter_d = {"global_id":relation["app_id"],"type":"app_device"}
                            known_item = self.mongo_client.get_one_document(filter_d)
                            if known_item:
                                if relation["device_id"] in known_item["devices"]:
                                    known_item["devices"].remove(relation["device_id"])
                                    self.mongo_client.update_document_relation(known_item,"app_device")
                                    plan_request[queue][relation_type].append(relation)
                        
                        if relation_type == "app_service" or relation_type == "service_resource":
                            plan_request[queue][relation_type].append(relation)                        
                    

            #SELF RULES SIMULATION
            self_simulation = False
            if self_simulation:
                MIN_APP_DEVICES = 3
                MIN_DEVICE_RESOURCES = 4
                OMITTED_DEVICES = [1]
                self.mongo_client.get_collection("relations")
                relations = self.mongo_client.get_all()
                devices_rules = []
                for relation in relations:
                    if relation["type"] == "device_resource" and relation["global_id"] not in OMITTED_DEVICES:
                        if len(relation["resources"])<MIN_DEVICE_RESOURCES:
                            str_alert = "The device with GID: "+str(relation["global_id"])+" doesn't have the required resources."
                            plan_request["ALERTS"]["warnings"].append(str_alert)
                            str_alert += " |"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            logger.info(str_alert)                        
                    elif relation["type"] == "app_device":
                        if len(relation["devices"])<MIN_APP_DEVICES:
                            str_alert = "The APP with GID: "+str(relation["global_id"])+" doesn't have the required devices."
                            plan_request["ALERTS"]["warnings"].append(str_alert)
                            str_alert += " |"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            logger.info(str_alert) 
        self.plan(plan_request)
           
        

    #Plan Process
    def plan(self,request):   
        execute_request = {}
        execute_request["CREATE_NODES"] = {"resource":[],"device":[],"app":[],"service":[]}
        execute_request["UPDATE_NODES"] = {"resource":[],"device":[],"app":[],"service":[]}
        execute_request["DELETE_NODES"] = {"resource":[],"device":[],"app":[],"service":[]}
        execute_request["CREATE_RELATION"] = {"device_resource":[],"app_device":[],"app_service":[],"service_resource":[]}
        execute_request["DELETE_RELATION"] = {"device_resource":[],"app_device":[],"app_service":[],"service_resource":[]}
        execute_request["ALERTS"] = {"warnings":[],"Failures":[]}
        execute_request["ALERTS"]["logs"] = request["ALERTS"]["logs"]
        execute_request["ALERTS"]["warnings"] = request["ALERTS"]["warnings"]

        #Nodes
        for queue in self.QUEUE_TYPES:
            for item_type in self.ITEMS_TYPES:
                if item_type != "property":
                    if queue == "create":
                        for item in request[queue][item_type]:
                            if item_type == "resource":
                                item_n = Resource(global_id=item["global_id"],name=item["name"],description=item["description"],type=item["resource_type"])
                            if item_type == "device":
                                item_n = [Device(global_id=item["global_id"],name=item["name"],description=item["description"],is_gateway=item["is_gateway"]),item["device_parent"]]
                            if item_type == "app":
                                item_n = App(global_id=item["global_id"],name=item["name"])
                            if item_type == "service":
                                item_n = Service(global_id=item["global_id"],name=item["name"])
                            execute_request["CREATE_NODES"][item_type].append(item_n)
                    if queue == "update":
                        for item in request[queue][item_type]:
                            if item_type == "resource":
                                node = Resource.nodes.get(global_id=item["global_id"])
                                new_node = self.update_resource_node(item,node)                        
                            if item_type == "device":
                                node = Device.nodes.get(global_id=item["global_id"])
                                new_node = self.update_device_node(item,node)
                            if item_type == "app":
                                node = App.nodes.get(global_id=item["global_id"])
                                new_node = self.update_app_node(item,node)
                            if item_type == "service":
                                node = Service.nodes.get(global_id=item["global_id"])
                                new_node = self.update_service_node(item,node)
                            execute_request["UPDATE_NODES"][item_type].append(new_node)
                    
                    if queue == "delete":
                        for item in request[queue][item_type]:
                            if item_type == "resource":
                                node = Resource.nodes.get(global_id=item["global_id"])
                            if item_type == "device":
                                node = Device.nodes.get(global_id=item["global_id"])
                            if item_type == "app":
                                node = App.nodes.get(global_id=item["global_id"])
                            if item_type == "service":
                                node = Service.nodes.get(global_id=item["global_id"])
                            execute_request["DELETE_NODES"][item_type].append(node)
            
            for relation_type in self.RELATIONS_TYPES:
                if queue == "create":
                    for item in request[queue][relation_type]:
                        execute_request["CREATE_RELATION"][relation_type].append(item)
                if queue == "delete":
                    for item in request[queue][relation_type]:
                        execute_request["DELETE_RELATION"][relation_type].append(item)        
        self.execute(execute_request)

    #Execute Process
    def execute(self,plan):
        connector = NeoConnector()

        #----Delete Relations ----#
        for relation_type in self.RELATIONS_TYPES:           
            for new_relation in plan["DELETE_RELATION"][relation_type]:
                if relation_type == "device_resource":
                    str_log = "DELETE "+relation_type.upper()+" DEVICE_ID: "+str(new_relation["device_id"])
                    str_log += " RESOURCE_ID: "+str(new_relation["resource_id"])
                    plan["ALERTS"]["logs"].append(str_log)
                    try:
                        resource = Resource.nodes.get(global_id=new_relation["resource_id"])
                        device = Device.nodes.get(global_id=new_relation["device_id"])
                        connector.delete_device_resource(device,resource)
                    except:
                        pass
                if relation_type == "app_device":
                    str_log = "DELETE "+relation_type.upper()+" APP_ID: "+str(new_relation["app_id"])
                    str_log += " DEVICE_ID: "+str(new_relation["device_id"])                  
                    plan["ALERTS"]["logs"].append(str_log)
                    try:
                        device = Device.nodes.get(global_id=new_relation["device_id"])
                        app = App.nodes.get(global_id=new_relation["app_id"])
                        connector.delete_app_device(app,device)
                    except:
                        pass
                
                if relation_type == "app_service":
                    str_log = "DELETE "+relation_type.upper()+" APP_ID: "+str(new_relation["app_id"])
                    str_log += " SERVICE_ID: "+str(new_relation["service_id"])                  
                    plan["ALERTS"]["logs"].append(str_log)
                    try:
                        service = Service.nodes.get(global_id=new_relation["service_id"])
                        app = App.nodes.get(global_id=new_relation["app_id"])
                        connector.delete_app_service(app,service)
                    except:
                        pass
                    
                if relation_type == "service_resource":
                    str_log = "DELETE "+relation_type.upper()+" SERVICE_ID: "+str(new_relation["service_id"])
                    str_log += " RESOURCE_ID: "+str(new_relation["resource_id"])                  
                    plan["ALERTS"]["logs"].append(str_log)
                    try:
                        service = Service.nodes.get(global_id=new_relation["service_id"])
                        resource = Resource.nodes.get(global_id=new_relation["resource_id"])
                        connector.delete_device_resource(app,service)
                    except:
                        pass

                str_log += " |"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logger.info(str_log)

        #---------------------------------------------#
        #new resources Nodes
        for item_type in self.ITEMS_TYPES:
            if item_type == "property":
                continue
            for new_node in plan["CREATE_NODES"][item_type]:                
                if item_type == "resource":
                    str_log = "CREATE "+item_type.upper()+" G_ID: "+str(new_node.global_id)
                    plan["ALERTS"]["logs"].append(str_log) 
                    connector.saveResource(new_node)
                if item_type == "device":
                    str_log = "CREATE "+item_type.upper()+" G_ID: "+str(new_node[0].global_id)
                    plan["ALERTS"]["logs"].append(str_log)
                    connector.saveDevice(new_node[0],new_node[1])
                if item_type == "app":
                    str_log = "CREATE "+item_type.upper()+" G_ID: "+str(new_node.global_id)
                    plan["ALERTS"]["logs"].append(str_log)
                    connector.saveApp(new_node)
                if item_type == "service":
                    str_log = "CREATE "+item_type.upper()+" G_ID: "+str(new_node.global_id)
                    plan["ALERTS"]["logs"].append(str_log)
                    connector.saveService(new_node)
                str_log += " |"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logger.info(str_log)
            for update_node in plan["UPDATE_NODES"][item_type]:                
                if item_type == "resource":
                    str_log = "UPDATE "+item_type.upper()+" G_ID: "+str(update_node.global_id)
                    plan["ALERTS"]["logs"].append(str_log)
                    connector.updateResource(update_node)
                if item_type == "device":
                    str_log = "UPDATE "+item_type.upper()+" G_ID: "+str(update_node[0].global_id)
                    plan["ALERTS"]["logs"].append(str_log)
                    connector.updateDevice(update_node[0],update_node[1])
                if item_type == "app":
                    str_log = "UPDATE "+item_type.upper()+" G_ID: "+str(update_node.global_id)
                    plan["ALERTS"]["logs"].append(str_log)
                    connector.updateApp(update_node)
                if item_type == "service":
                    str_log = "UPDATE "+item_type.upper()+" G_ID: "+str(update_node.global_id)
                    plan["ALERTS"]["logs"].append(str_log)
                    connector.updateApp(update_node)
                str_log += " |"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logger.info(str_log)
            for delete_node in plan["DELETE_NODES"][item_type]:                
                if item_type == "resource":
                    str_log = "DELETE "+item_type.upper()+" G_ID: "+str(delete_node.global_id)
                    plan["ALERTS"]["logs"].append(str_log)
                    connector.deleteResource(delete_node)
                if item_type == "device":
                    str_log = "DELETE "+item_type.upper()+" G_ID: "+str(delete_node.global_id)
                    plan["ALERTS"]["logs"].append(str_log)
                    connector.deleteDevice(delete_node)
                if item_type == "app":
                    str_log = "DELETE "+item_type.upper()+" G_ID: "+str(delete_node.global_id)
                    plan["ALERTS"]["logs"].append(str_log)
                    connector.deleteApp(delete_node)
                if item_type == "service":
                    str_log = "DELETE "+item_type.upper()+" G_ID: "+str(delete_node.global_id)
                    plan["ALERTS"]["logs"].append(str_log)
                    connector.deleteService(delete_node)
                str_log += " |"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logger.info(str_log)
        
        for relation_type in self.RELATIONS_TYPES:
            for new_relation in plan["CREATE_RELATION"][relation_type]:
                if relation_type == "device_resource":
                    str_log = "CREATE "+relation_type.upper()+" DEVICE_ID: "+str(new_relation["device_id"])
                    str_log += " RESOURCE_ID: "+str(new_relation["resource_id"])
                    plan["ALERTS"]["logs"].append(str_log)
                    resource = Resource.nodes.get(global_id=new_relation["resource_id"])
                    device = Device.nodes.get(global_id=new_relation["device_id"])
                    connector.save_device_resource(device,resource)
                if relation_type == "app_device":
                    str_log = "CREATE "+relation_type.upper()+" APP_ID: "+str(new_relation["app_id"])
                    str_log += " DEVICE_ID: "+str(new_relation["device_id"])                  
                    plan["ALERTS"]["logs"].append(str_log)
                    device = Device.nodes.get(global_id=new_relation["device_id"])
                    app = App.nodes.get(global_id=new_relation["app_id"])
                    connector.save_app_device(app,device)
                if relation_type == "app_service":
                    str_log = "CREATE "+relation_type.upper()+" APP_ID: "+str(new_relation["app_id"])
                    str_log += " SERVICE_ID: "+str(new_relation["service_id"])                  
                    plan["ALERTS"]["logs"].append(str_log)
                    service = Service.nodes.get(global_id=new_relation["service_id"])
                    app = App.nodes.get(global_id=new_relation["app_id"])
                    connector.save_app_service(app,service)
                if relation_type == "service_resource":
                    str_log = "CREATE "+relation_type.upper()+" SERVICE_ID: "+str(new_relation["service_id"])
                    str_log += " RESOURCE_ID: "+str(new_relation["resource_id"])                  
                    plan["ALERTS"]["logs"].append(str_log)
                    service = Service.nodes.get(global_id=new_relation["service_id"])
                    resource = Resource.nodes.get(global_id=new_relation["resource_id"])
                    connector.save_service_resource(service,resource)
                str_log += " |"+datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logger.info(str_log)           

        for log in plan["ALERTS"]["logs"]:
            self.send_message(log)
        for alert in plan["ALERTS"]["warnings"]:
            self.send_message(alert)





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
        if new_data["global_id"] != old_node.global_id: 
            old_node.global_id = new_data["global_id"]
        if new_data["name"] != old_node.name: 
            old_node.name = new_data["name"]
        if new_data["description"] != old_node.description: 
            old_node.description = new_data["description"]
        if new_data["is_gateway"] != old_node.is_gateway:
            old_node.is_gateway = new_data["is_gateway"]
        for parent in old_node.device_parent:
            if new_data["device_parent"] != parent.global_id:
                device_parent = new_data["device_parent"]      
        return [old_node, device_parent]
    
    def update_resource_node(self,new_data,old_node):
        if new_data["global_id"] != old_node.global_id: 
            old_node.global_id = new_data["global_id"]
        if new_data["name"] != old_node.name:
            old_node.name = new_data["name"]
        if new_data["description"] != old_node.description: 
            old_node.description = new_data["description"]
        if new_data["resource_type"] != old_node.type: 
            old_node.type = new_data["resource_type"]
        return old_node
    
    
    def update_app_node(self,new_data,old_node):
        if new_data["global_id"] != old_node.global_id: 
            old_node.global_id = new_data["global_id"]
        if new_data["name"] != old_node.name: 
            old_node.name = new_data["name"]
        return old_node
    
    def update_service_node(self,new_data,old_node):
        if new_data["global_id"] != old_node.global_id: 
            old_node.global_id = new_data["global_id"]
        if new_data["name"] != old_node.name: 
            old_node.name = new_data["name"]
        return old_node
    
    #Send AMQP Broker data
    def send_message(self,message):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='logs')
        channel.basic_publish(exchange='', routing_key='logs', body=message)
        connection.close()



@app.command()
def main():
    auto_component = SelfDescriptionComponent()
    auto_component.init_mapek()

#Principal Process
if __name__ == '__main__':
    try:
        app()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)