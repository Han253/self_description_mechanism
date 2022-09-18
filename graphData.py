from telnetlib import GA
from neomodel import config
from neomodel import (config, StructuredNode, StringProperty, IntegerProperty, 
    BooleanProperty,UniqueIdProperty, RelationshipTo)

import os

# URL configuration
NEO_HOST = os.getenv("NEO_HOST", default='localhost')
config.DATABASE_URL = 'bolt://neo4j:admin@'+NEO_HOST+':7687'

class Service(StructuredNode):
    nid = UniqueIdProperty()
    global_id = IntegerProperty(unique_index=True, required=True)
    name = StringProperty(unique_index=True, required=True)    
    resources = RelationshipTo('Resource',"USES")
    apps = RelationshipTo('App',"EMPLOY")

class App(StructuredNode):
    nid = UniqueIdProperty()
    global_id = IntegerProperty(unique_index=True, required=True)
    name = StringProperty(unique_index=True, required=True)    
    devices = RelationshipTo('Device',"GROUP")
    services = RelationshipTo('Service',"EMPLOY")

class Device(StructuredNode):
    nid = UniqueIdProperty()
    global_id = IntegerProperty(unique_index=True, required=True)
    name = StringProperty(unique_index=True, required=True)
    description = StringProperty()
    is_gateway = BooleanProperty()
    device_parent = RelationshipTo('Device',"PARENT")
    resources = RelationshipTo('Resource',"HAS")
    apps = RelationshipTo('App',"GROUP")

class Resource(StructuredNode):
    nid = UniqueIdProperty()
    global_id = IntegerProperty(unique_index=True, required=True)
    name = StringProperty(unique_index=True, required=True)
    description = StringProperty()
    type = StringProperty()
    devices = RelationshipTo("Device", "HAS")
    services = RelationshipTo("Service", "USES")

class Gateway(Device):
    pass

class Sensor(Resource):
    pass

class Actuator(Resource):
    pass

class Communication(Resource):
    pass

class Storage(Resource):
    pass
    

class NeoConnector():

    #Save nodes
    def saveResource(self,resource:Resource):
        if resource.type == "SENSOR":
            node = Sensor(global_id=resource.global_id,name=resource.name,description=resource.description,type=resource.type)
        if resource.type == "ACTUATOR":
            node = Actuator(global_id=resource.global_id,name=resource.name,description=resource.description,type=resource.type)
        if resource.type == "COMMUNICATION":
            node = Communication(global_id=resource.global_id,name=resource.name,description=resource.description,type=resource.type)
        if resource.type == "STORAGE":
            node = Storage(global_id=resource.global_id,name=resource.name,description=resource.description,type=resource.type)
        
        node = node.save()
        return node
    
    def updateResource(self,resource:Resource):
        resource = resource.save()
        return resource
    
    def deleteResource(self,resource:Device):
        resource.devices.disconnect_all()
        resource.delete()

    def saveDevice(self,device:Device,parent_global_id=None):
        
        if device.is_gateway:
            gateway = Gateway(global_id=device.global_id,name=device.name, description=device.description,is_gateway=device.is_gateway)
            gateway.save()
            if parent_global_id!= None:
                device_parent_node = Device.nodes.get(global_id= parent_global_id)
                gateway.device_parent.connect(device_parent_node)        
            return gateway

        else:
            device = device.save()
            if parent_global_id!= None:
                device_parent_node = Device.nodes.get(global_id= parent_global_id)
                device.device_parent.connect(device_parent_node)        
            return device  

    def updateDevice(self,device:Device,parent_global_id=None):
        device = device.save()
        if parent_global_id!= None:
            device.device_parent.disconnect_all()
            device_parent_node = Device.nodes.get(global_id= parent_global_id)
            device.device_parent.connect(device_parent_node)
    
    def deleteDevice(self,device:Device):
        device.device_parent.disconnect_all()
        device.delete()
    
    def saveApp(self,app:App):
        app = app.save()
        return app
    
    def saveService(self,service:Service):
        service = service.save()
        return service
    
    def updateApp(self,app:App):
        app = app.save()
        return app
    
    def updateService(self,service:Service):
        service = service.save()
        return service

    def deleteApp(self,app:App):
        app.devices.disconnect_all()
        app.services.disconnect_all()
        app.delete()

    def deleteService(self,service:Service):
        service.resources.disconnect_all()
        service.apps.disconnect_all()
        service.delete()
    
    def save_device_resource(self,device:Device,resource:Resource):
        resource_r = device.resources.search(global_id=resource.global_id)
        if not resource_r:
            device.resources.connect(resource)
    
    def save_app_device(self,app:App,device:Device):
        device_r = app.devices.search(global_id=device.global_id)
        if not device_r:
            app.devices.connect(device)

    def save_app_service(self,app:App,service:Service):
        service_r = app.services.search(global_id=service.global_id)
        if not service_r:
            app.services.connect(service)
    
    def save_service_resource(self,service:Service,resource:Resource):
        resource_r = service.resources.search(global_id=resource.global_id)
        if not resource_r:
            service.resources.connect(resource)
    
    def delete_device_resource(self,device:Device,resource:Resource):
        resource_r = device.resources.search(global_id=resource.global_id)
        if resource_r:
            device.resources.disconnect(resource)
    
    def delete_app_device(self,app:App,device:Device):
        device_r = app.devices.search(global_id=device.global_id)
        if device_r:
            app.devices.disconnect(device)
    
    def delete_app_service(self,app:App,service:Service):
        service_r = app.services.search(global_id=service.global_id)
        if service_r:
            app.services.disconnect(service)
    
    def delete_service_resource(self,service:Service,resource:Resource):
        resource_r = service.resources.search(global_id=resource.global_id)
        if resource_r:
            service.resources.disconnect(resource)

    

