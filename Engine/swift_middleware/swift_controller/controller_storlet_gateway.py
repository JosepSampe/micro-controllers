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
        self.file_path = None
        self.storlet_docker_gateway = None
        self.storlet_metadata = None
        self.storlet_name = None
        
   
    def getAccountInfo(self):
        return cc.getAccountMetadata(self.account)
        
    
    def authorizeStorletExecution(self,storlet):
        resp = cc.makeSwiftRequest("HEAD",self.account, self.hconf["storlet_container"], storlet)
        if resp.status_int < 300 and resp.status_int >= 200:
            self.storlet_metadata = resp.headers
            self.storlet_name = storlet
            return True
        return False
               
        
    def executeStorlet(self, orig_resp, params, input_pipe=None):
        self.storlet_docker_gateway = StorletGatewayDocker(self.hconf, self.logger, self.app, 
                                                           self.version, self.account, self.container, 
                                                           self.obj)
        
        # Set Storlet Metadata to storletgateway
        self.storlet_docker_gateway.storlet_metadata = self.storlet_metadata
        
        # Simulate Storlet request
        new_env = dict(orig_resp.environ)     
        req = Request.blank(new_env['PATH_INFO'], new_env)
        req.headers['X-Run-Storlet'] = self.storlet_name
        self.storlet_docker_gateway.augmentStorletRequest(req)
        req.environ['QUERY_STRING'] = params
        
        # Execute Storlet request
        (out_md, app_iter) = self.storlet_docker_gateway.gatewayObjectGetFlow(req, self.container, 
                                                                              self.obj, orig_resp, 
                                                                              input_pipe)
        out_fd = app_iter.obj_data
        
        return out_fd, app_iter
    
    
    def executeStorletOnProxy(self, orig_resp, params, input_pipe=None):
        self.storlet_docker_gateway = StorletGatewayDocker(self.hconf, self.logger, self.app, 
                                                           self.version, self.account, self.container, 
                                                           self.obj)
   
        # Set Storlet Metadata to storletgateway
        self.storlet_docker_gateway.storlet_metadata = self.storlet_metadata
        
        # Simulate Storlet request
        new_env = dict(orig_resp.environ)     
        req = Request.blank(new_env['PATH_INFO'], new_env)
        req.headers['X-Run-Storlet'] = self.storlet_name      
        self.storlet_docker_gateway.augmentStorletRequest(req)
        req.environ['QUERY_STRING'] = params       
        
        # Execute Storlet request
        (out_md, app_iter) = self.storlet_docker_gateway.gatewayProxyGETFlow(req, self.container, 
                                                                             self.obj, orig_resp, 
                                                                             input_pipe)
        out_fd = app_iter.obj_data
        
        return out_fd, app_iter

