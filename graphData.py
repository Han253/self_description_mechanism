from neomodel import config
from neomodel import (config, StructuredNode, StringProperty, IntegerProperty, 
    BooleanProperty,UniqueIdProperty, RelationshipTo)

# URL configuration
config.DATABASE_URL = 'bolt://neo4j:admin@localhost:7687'

class Device(StructuredNode):
    nid = UniqueIdProperty()
    global_id = IntegerProperty(unique_index=True, required=True)
    name = StringProperty(unique_index=True, required=True)
    description = StringProperty()
    is_gateway = BooleanProperty()
    device_parent = RelationshipTo('Device',"PARENT")

class NeoConnector():

    def saveDevice(self,device:Device,parent_global_id=None):
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

    

