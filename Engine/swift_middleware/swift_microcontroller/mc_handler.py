'''===========================================================================
15-Oct-2015    josep.sampe    Initial implementation.
==========================================================================='''
from swift.common.swob import wsgify
from swift.common.swob import HTTPBadRequest
from swift.common.swob import HTTPUnauthorized
from swift.common.swob import HTTPAccepted
from swift.common.swob import Response
from swift.common.swob import Request
from swift.common.utils import get_logger, is_success, cache_from_env
import ConfigParser
import mc_docker_gateway as cdg
import mc_storlet_gateway as csg
import mc_common as cc
import pickle

class SwiftControllerMiddleware(object):
    def __init__(self, app, conf):
        self.memcache = None
        self.app = app
        self.execution_server = conf.get('execution_server')
        self.logger = get_logger(conf, log_route='swift_controller')
        self.hconf = conf
        self.containers = [conf.get('mc_container'),
                           conf.get('mc_dependency'),
                           conf.get('storlet_container'),
                           conf.get('storlet_dependency')]
        self.available_triggers = ['X-Controller-Onget',
                                   'X-Controller-Ondelete',
                                   'X-Controller-Onput',
                                   'X-Controller-Ontimer']
        self.logger.debug('Swift Controller - Init OK')

    @wsgify
    def __call__(self, req):
        
        try:
            if self.execution_server == 'proxy':
 
                self.logger.info('Swift Controller - Proxy Server execution')
                version, account, container, obj = req.split_path(2, 4, rest_with_last=True)
                
                
                if req.method == 'GET':
                    if self.memcache is None:
                        self.memcache = cache_from_env(req.environ)
      
                    cached = self.memcache.get(account+"/"+container+"/"+obj)
                                        
                    self.logger.info('Swift Controller - Checking in cache: '+account+"/"+container+"/"+obj)
                    
                    if cached is not None:
                        value = pickle.loads(cached)
                    
                        self.logger.info('Swift Controller - *+---------- OBJECT IN CACHE -----------+*')
                        # Return Cached
                        old_env = req.environ.copy()
                        orig_req = Request.blank(old_env['PATH_INFO'],old_env)
                                               
                        resp_headers = value["Headers"]
                        resp_headers['Content-Length'] = None
                        
                        return Response(body=value["Body"],
                                        headers=resp_headers,
                                        request=req,
                                        conditional_response=True)
                              
            else: # OBJECT SERVER
                device, partition, account, container, obj = req.split_path(5, 5, rest_with_last=True)
                version = '0'
                
        except Exception as e:
            self.logger.info(e)
            return req.get_response(self.app)
        

        """
        ########### PROXY SERVER CASE: PRE-PROCESSING -> PUT METADATA FILE
        """
        
        # ASSING HANDLER AND PUT METADADA
        if self.execution_server == 'proxy' and req.method == 'PUT' and \
            ( any((True for x in self.available_triggers if x in req.headers.keys())) ):
 
            header = [i for i in self.available_triggers if i in req.headers.keys()]
            if len(header) > 1:
                return HTTPUnauthorized('The system can only set 1 micro-controller each time.\n')
            mc = req.headers[header[0]]
            
            # Verify if Micro-controller is in Swift
            if not cc.verify_access(self, req.environ, version, account, self.hconf["handler_container"], mc):
                return HTTPUnauthorized('Handler error: Perhaps '+mc+' doesn\'t exists in Swift.\n')
            
            # Verify if object is in Swift
            if not cc.verify_access(self, req.environ, version, account, container, obj):
                return HTTPUnauthorized('Object error: Perhaps '+obj+' doesn\'t exists in Swift.\n')
  
        """
        ########### OBJECT SERVER CASE: PRE-PROCESSING -> PUT METADATA FILE
        """

        if self.execution_server == 'object' and req.method == 'PUT' and \
            ( any((True for x in self.available_triggers if x in req.headers.keys())) ):
            
            header = [i for i in self.available_triggers if i in req.headers.keys()]
            if len(header) > 1:
                return HTTPUnauthorized('The system can only set 1 controller each time.\n')
            mc = req.headers[header[0]]
             
            # Put to Get to get physical location of main file   
            get_req = req.copy_get()
            get_resp = get_req.get_response(self.app)

            docker_gateway = cdg.ControllerGatewayDocker(req, get_resp, self.hconf, self.logger, self.app, 
                                                         device, partition, account, container, obj)
            #write micro-controller to file metadata
            docker_gateway.set_microcontroller(header[0],mc)


            return HTTPAccepted('Metadata file saved correctly.\n')
        
        # -----------------------------------------------------------------------------------------------------------------
        # -----------------------------------------------------------------------------------------------------------------
        orig_resp = req.get_response(self.app)   
        # -----------------------------------------------------------------------------------------------------------------
        # -----------------------------------------------------------------------------------------------------------------

        """
        ################################### OBJECT SERVER CASE: POST-PROCESSING ############################################
        """             
        if self.execution_server == 'object':
            
            #print "----------------------------------------------------"
            #print orig_resp.app_iter  #TODO: THERE IS A BUG
            #print "----------------------------------------------------"
        
            
            self.logger.info('Swift Controller - Object Server execution')
            
            #check if is a valid request
            if not self.valid_request(req, container):
                # We only want to process PUT and GET requests
                # Also we ignore the calls that goes to the storlet, handler and dependency container  
                return orig_resp
            
            
            docker_gateway = cdg.ControllerGatewayDocker(req, orig_resp, self.hconf, self.logger, self.app, 
                                                         device, partition, account, container, obj)
            
            if docker_gateway.get_microcontrollers():
                self.logger.info('Swift Controller - There are micro-controllers to execute')
                
                # We need to start Internal CLient
                cc.start_internal_client_daemon()
                # We need to start container if it is stopped
                docker_gateway.start_container()  # TODO: NO SEMPRE

                # Go to run the micro-controller         
                storlet_list = docker_gateway.execute_controller_handlers()     
                
                # Go to run the Storlet whether the microcontroller sends back any.
                if storlet_list: 
                    storlet_gateway = csg.ControllerGatewayStorlet(self.hconf, self.logger, self.app, account, container, obj)
                    account_meta = storlet_gateway.get_account_info()
                    out_fd = None
                    toProxy = 0
                      
                    # Verify if the account can execute Storlets    
                    storlets_enabled = account_meta.get('x-account-meta-storlet-enabled','False')
                    
                    if storlets_enabled == 'False':
                        self.logger.info('Swift Controller - Account disabled for storlets')
                        return HTTPBadRequest('Swift Controller - Account disabled for storlets')
                    
                    
                    # Execute multiple Storlets, PIPELINE, if any.
                    for key in sorted(storlet_list):
                        self.logger.info('************************ VISUAL STORLET EXECUTION DIVISOR ***************************')
                        
                        # Get Storlet and parameters
                        storlet, parameters = storlet_list[key]["Storlet"].items()[0]                        
                        nodeToExecute = storlet_list[key]["NodeToExecute"]
                        
                        self.logger.info('Swift Controller - Go to execute '+storlet+' storlet with parameters"' \
                                         +parameters+'"'+ " on "+ nodeToExecute)
                        
                        if nodeToExecute == "object-server":
                            if not storlet_gateway.authorize_storlet_execution(storlet):
                                return HTTPUnauthorized('Swift Controller - Storlet: No permission')
    
                            old_env = req.environ.copy()
                            orig_req = Request.blank(old_env['PATH_INFO'], old_env)
                            out_fd, app_iter = storlet_gateway.execute_storlet_on_object(orig_resp,parameters,out_fd)
                            
                            # Notify to the Proxy that Storlet was executed in the object-server
                            orig_resp.headers["Storlet-Executed"] = "True"
                        
                        else:
                            orig_resp.headers["Storlet-Execute-On-Proxy-"+str(toProxy)] = storlet
                            orig_resp.headers["Storlet-Execute-On-Proxy-Parameters-"+str(toProxy)] = parameters
                            toProxy = toProxy + 1
                            orig_resp.headers["Total-Storlets-To-Execute-On-Proxy"] = toProxy
                    
                    
                    # Delete headers for the correct working of the Storlet framework
                    if 'Content-Length' in orig_resp.headers:
                        orig_resp.headers.pop('Content-Length')
                    if 'Transfer-Encoding' in orig_resp.headers:
                        orig_resp.headers.pop('Transfer-Encoding')
  
                    # Return Storlet response
                    return Response(app_iter=app_iter,
                                    headers=orig_resp.headers,
                                    request=orig_req,
                                    conditional_response=True)
                else:
                    self.logger.info('Swift Controller - No Storlets to execute')
            else:    
                self.logger.info('Swift Controller - No micro-controllers to execute')
        
            self.logger.info("Object Path: "+orig_resp.app_iter._data_file.rsplit('/', 1)[0])
         
        
        """
        ################################### PROXY SERVER CASE: POST-PROCESSING ############################################
        """
        
        if self.execution_server == 'proxy' and 'Total-Storlets-To-Execute-On-Proxy' in orig_resp.headers:

            self.logger.info('Swift Controller - There are Storlets to execute from object server micro-controller')
            
            storlet_gateway = csg.ControllerGatewayStorlet(self.hconf, self.logger, self.app, account, container, obj)
            account_meta = storlet_gateway.get_account_info()     
            out_fd = None
              
            # Verify if the account can execute Storlets    
            storlets_enabled = account_meta.get('x-account-meta-storlet-enabled','False')
            
            if storlets_enabled == 'False':
                self.logger.info('Swift Controller - Account disabled for storlets')
                return HTTPBadRequest('Swift Controller - Account disabled for storlets')

            for index in range(int(orig_resp.headers["Total-Storlets-To-Execute-On-Proxy"])):
                self.logger.info('************************ VISUAL STORLET EXECUTION DIVISOR ***************************')
                storlet = orig_resp.headers["Storlet-Execute-On-Proxy-"+str(index)]
                parameters = orig_resp.headers["Storlet-Execute-On-Proxy-Parameters-"+str(index)]
                
                self.logger.info('Swift Controller - Go to execute '+storlet+' storlet with parameters "'+parameters+'"')
                
                if not storlet_gateway.authorize_storlet_execution(storlet):
                    return HTTPUnauthorized('Swift Controller - Storlet: No permission')

                old_env = req.environ.copy()
                orig_req = Request.blank(old_env['PATH_INFO'], old_env)
                out_fd, app_iter = storlet_gateway.execute_storlet_on_proxy(orig_resp, parameters, out_fd)
                
                
             
            old_env = req.environ.copy()
            orig_req = Request.blank(old_env['PATH_INFO'], old_env)
            resp_headers = orig_resp.headers

            resp_headers['Content-Length'] = None
            
            return Response(app_iter=app_iter,
                            headers=resp_headers,
                            request=orig_req,
                            conditional_response=True)
            
        
        # NO STORTLETS TO EXECUTE ON PROXY 
        elif self.execution_server == 'proxy' and req.method == "GET" and \
            (orig_resp.headers["Storlet-Executed"] or "X-Object-Meta-Run-Micro-Controller" in orig_resp.headers):
            
            self.logger.info('Swift Controller - There are NO Storlets to execute from object server micro-controller')           
                        
                        
            # We must execute the micro-controller here in the proxy
            if "X-Object-Meta-Run-Micro-Controller" in orig_resp.headers: 

                micro_controller = orig_resp.headers["X-Object-Meta-Run-Micro-Controller"]
                orig_resp.headers["X-Object-Meta-Path"] = "http://"+req.host+req.path_info
                
                docker_gateway = cdg.ControllerGatewayDocker(req, orig_resp, self.hconf, self.logger, self.app, 
                                                             None, None, account, container, obj)
                
                #RUN MICROCONTROLLER HERE
                docker_gateway.set_microcontroller_list(micro_controller)
                
                self.logger.info('Swift Controller - There are micro-controllers to execute')
                
                # We need to start Internal CLient
                cc.start_internal_client_daemon()
                # We need to start container if it is stopped
                docker_gateway.startContainer()
                
                # Go to run the micro-controller         
                storlet_list = docker_gateway.executeControllerHandlers("proxy")
            
                orig_resp.headers.pop("X-Object-Meta-Run-Micro-Controller")
                orig_resp.headers.pop("X-Object-Meta-Micro-Controller-Data")
                orig_resp.headers.pop("X-Object-Meta-Path")
                
            
            
            if 'Transfer-Encoding' in orig_resp.headers:
                orig_resp.headers.pop('Transfer-Encoding')

            if is_success(orig_resp.status_int):
                old_env = req.environ.copy()
                orig_req = Request.blank(old_env['PATH_INFO'],old_env)
                resp_headers = orig_resp.headers

                resp_headers['Content-Length'] = None
                
                return Response(app_iter=orig_resp.app_iter,
                                headers=resp_headers,
                                request=orig_req,
                                conditional_response=True)
  
        return orig_resp

    def valid_request(self, req, container):
        if req.method == 'GET' and container not in self.containers:
            #Also we need to discard the copy calls.    
            if not "HTTP_X_COPY_FROM" in req.environ.keys():
                self.logger.info('Swift Controller - Valid req: OK!')        
                return True
            
        self.logger.info('Swift Controller - Valid req: NO!')        
        return False


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""
    conf = global_conf.copy()
    conf.update(local_conf)
    
    mc_conf = dict()
    mc_conf['execution_server'] = conf.get('execution_server','object')
    mc_conf['controller_timeout'] = conf.get('controller_timeout', 20)
    mc_conf['controller_pipe'] = conf.get('controller_pipe', 
                                          'controller_pipe')
    mc_conf['mc_dir'] = conf.get('mc_dir','/home/lxc_device/handlers/scopes')
    
    mc_conf['mc_container'] = conf.get('mc_container','handler')
    mc_conf['mc_dependency'] = conf.get('mc_dependency','dependency')
    
    mc_conf['storlet_timeout'] = conf.get('storlet_timeout',40) 
    mc_conf['storlet_container'] = conf.get('storlet_container','storlet')
    mc_conf['storlet_dependency'] = conf.get('storlet_dependency', 
                                             'dependency')
    
    mc_conf['docker_repo'] = conf.get('docker_repo','192.168.2.1:5001')
       
    configParser = ConfigParser.RawConfigParser()
    configParser.read(conf.get('storlet_gateway_conf', 
                               '/etc/swift/storlet_docker_gateway.conf'))
    
    additional_items = configParser.items("DEFAULT")
    for key, val in additional_items:
        mc_conf[key] = val

    def swift_controller(app):
        return SwiftControllerMiddleware(app, mc_conf)

    return swift_controller

