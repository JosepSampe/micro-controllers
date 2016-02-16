from swiftclient import client as c


def put_handler_object(url, token, handler_name, handler_path, main_class, dependency):
       
    metadata = {'X-Object-Meta-Handler-Language':'Java',
                'X-Object-Meta-Handler-Interface-Version':'1.0',
                'X-Object-Meta-Handler-Library-Dependency': dependency, 
                'X-Object-Meta-Handler-Handler-Dependency':'no',
                'X-Object-Meta-Handler-Main': main_class}
    f = open('%s/%s' % (handler_path, handler_name),'r')
    content_length = None
    response = dict()

    c.put_object(url, token, 'handler', handler_name, f, 
                 content_length, None, None, 
                 "application/octet-stream", metadata, 
                 None, None, None, response)
    f.close()
    status = response.get('status') 
    assert (status == 200 or status == 201)

    
'''------------------------------------------------------------------------'''
def put_handler_dependency(url, token, dep_name, local_path_to_dep):
    metadata = {'X-Object-Meta-Storlet-Dependency-Version': '1'}
    f = open('%s/%s' %(local_path_to_dep, dep_name),'r')
    content_length = None
    response = dict()
    c.put_object(url, token, 'dependency', dep_name, f, 
                 content_length, None, None, "application/octet-stream", 
                 metadata, None, None, None, response)
    f.close()
    status = response.get('status') 
    assert (status == 200 or status == 201)
    
    
AUTH_IP = 'iostack.urv.cat'
AUTH_PORT = '5000'
ACCOUNT = 'josep'
USER_NAME = 'josep'
PASSWORD = 'jsampe'

#AUTH_IP = '10.30.239.240'
#ACCOUNT = 'service'
#USER_NAME = 'swift'
#PASSWORD = 'urv'

os_options = {'tenant_name': ACCOUNT}

url, token = c.get_auth("http://" + AUTH_IP + ":" + AUTH_PORT + "/v2.0", ACCOUNT +":"+USER_NAME, PASSWORD, os_options = os_options, auth_version="2.0")

"""-------------------------------------------------------------------------------------------"""
print token


#Prefetching
put_handler_object(url, token,'prefetching-1.0.jar','/home/josep/Josep/workspace/Handler_WebPrefetching/bin' ,'com.urv.handler.webprefetching.PrefetchingHandler', 'json-simple-1.1.1.jar')
put_handler_dependency(url, token,'json-simple-1.1.1.jar','/home/josep/Josep/workspace/Handler_WebPrefetching/lib')

#COUNTER
#put_handler_object(url, token,'counter-1.0.jar','/home/josep/Josep/workspace/Handler_Counter/bin' ,'com.urv.handler.counter.CounterHandler', 'json-simple-1.1.1.jar')
#put_handler_dependency(url, token,'json-simple-1.1.1.jar','/home/josep/Josep/workspace/Handler_Counter/lib')

#TransGrep
#put_handler_object(url, token,'transgrep-1.0.jar','/home/josep/Josep/workspace/Handler_TransGrep/bin' ,'com.urv.handler.transgrep.TransGrepHandler', 'json-simple-1.1.1.jar')

#CBAC
#put_handler_object(url, token,'cbac-1.0.jar','/home/josep/Josep/workspace/Handler_CBAC/bin' ,'com.urv.handler.cbac.CBACHandler', 'json-simple-1.1.1.jar')
