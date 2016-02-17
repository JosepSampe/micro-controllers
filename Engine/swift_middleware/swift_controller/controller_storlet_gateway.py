'''===========================================================================
16-Oct-2015    josep.sampe    Initial implementation.
05-Feb-2016    josep.sampe    Added Proxy execution.
==========================================================================='''
from swift.common.swob import Request
from storlet_gateway.storlet_docker_gateway import StorletGatewayDocker
import controller_common as cc


class ControllerGatewayStorlet():

    def __init__(self,hconf, logger, app, account, container, obj):
        self.hconf = hconf
        self.logger = logger
        self.app = app
        self.version = 0   
        self.account = account
        self.container = container
        self.obj = obj
        self.gateway = None
        self.storlet_metadata = None
        self.storlet_name = None
        
   
    def getAccountInfo(self):
        return cc.getAccountMetadata(self.account)
        
    
    def authorizeStorletExecution(self,storlet):
        resp = cc.makeSwiftRequest("HEAD",self.account, 
                                   self.hconf["storlet_container"], 
                                   storlet)
        if resp.status_int < 300 and resp.status_int >= 200:
            self.storlet_metadata = resp.headers
            self.storlet_name = storlet
            return True
        return False
      
      
    def set_storlet_request(self,orig_resp,params):
        self.gateway = StorletGatewayDocker(self.hconf, self.logger, self.app, 
                                            self.version, self.account, 
                                            self.container, self.obj)
        
        # Set Storlet Metadata to storletgateway
        self.gateway.storlet_metadata = self.storlet_metadata
  
        # Simulate Storlet request
        new_env = dict(orig_resp.environ)     
        req = Request.blank(new_env['PATH_INFO'], new_env)
        req.headers['X-Run-Storlet'] = self.storlet_name
        self.gateway.augmentStorletRequest(req)
        req.environ['QUERY_STRING'] = params         
        
        return req
    
    def executeStorletOnObject(self, orig_resp, params, input_pipe=None):   
        req = self.set_storlet_rquest(orig_resp, params)
                
        # Execute Storlet request
        (_, app_iter) = self.gateway.gatewayObjectGetFlow(req, self.container, 
                                                          self.obj, orig_resp, 
                                                          input_pipe)  
        return app_iter.obj_data, app_iter
    
    
    def executeStorletOnProxy(self, orig_resp, params, input_pipe=None):
        req = self.set_storlet_rquest(orig_resp, params)      
        
        # Execute Storlet request
        (_, app_iter) = self.gateway.gatewayProxyGETFlow(req, self.container, 
                                                         self.obj, orig_resp, 
                                                         input_pipe)        
        return app_iter.obj_data, app_iter

