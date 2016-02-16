'''===========================================================================
15-Oct-2015    josep.sampe    Initial implementation.
==========================================================================='''
from swift.common.swob import wsgify
from swift.common.swob import HTTPBadRequest
from swift.common.swob import HTTPUnauthorized
from swift.common.swob import Response
from swift.common.swob import Request
from swift.common.utils import get_logger, is_success, cache_from_env

import ConfigParser
import controller_docker_gateway as cdg
import controller_storlet_gateway as csg
import controller_common as cc
import os
import ast
import pickle


class SwiftControllerMiddleware(object):
    def __init__(self, app, conf):
        self.app = app
        self.execution_server = conf.get('execution_server')
        self.logger = get_logger(conf, log_route='swift_controller')
        self.logger.info('Swift Controller - Init OK')
        self.hconf = conf
        self.available_triggers = ['X-Controller-Onget','X-Controller-Ondelete']
        self.memcache = None

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
                   
                """ 
                # Virtual Folder
                if "X-Use-Controller" in req.headers:
                    if len(obj.rsplit('/',1)) > 1:
                        virtual_folder = container+"/"+obj.rsplit('/',1)[0]
                        obj = obj.rsplit('/',1)[1]
                    else:
                        virtual_folder = container
                    container = self.hconf["root_container"] 
                """             
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
        if self.execution_server == 'proxy' and req.method == 'PUT' and ( any((True for x in self.available_triggers if x in req.headers.keys())) ):
 
            header = [i for i in self.available_triggers if i in req.headers.keys()]
            if len(header) > 1:
                return HTTPUnauthorized('The system can only set 1 controller each time.\n')
            handler = req.headers[header[0]]
            
            # Verify if handler is in Swift
            if not cc.verify_access(self, req.environ, version, account, self.hconf["handler_container"], handler):
                return HTTPUnauthorized('Handler error: Perhaps '+handler+' doesn\'t exists in Swift.\n')
            
            # Verify if object is in Swift
            if not cc.verify_access(self, req.environ, version, account, container, obj):
                return HTTPUnauthorized('Object error: Perhaps '+obj+' doesn\'t exists in Swift.\n')
        

        """
        ************ PROXY SERVER CASE: PRE-PROCESSING -> VIRTUAL FOLDER
        """
        """
        if self.execution_server == 'proxy' and "X-Use-Controller" in req.headers:
            
            print "-------------- PROXY: USE CONTROLLER ----------------------"
            
            if req.method == 'PUT' :
                
                if not cc.verify_access(self, req.environ, version, account, container, virtual_folder): # Chack access to virtual folder
                    self.logger.info('Folder error: Perhaps '+account+"/"+container+"/"+virtual_folder+' doesn\'t exists in Swift.')
                    cc.create_virtual_folder(self, req.environ, version, account, container, virtual_folder)
                
                
                resp, success = cc.save_file(self,req.environ, version, account, "CONTROLLER-"+self.hconf["default_policy"] , virtual_folder+"/"+obj, "/"+container+"/"+virtual_folder) # Save original file
                if success:
                    cc.update_virtual_folder(self, req.environ, version, account, container, virtual_folder, obj, "CONTROLLER-"+self.hconf["default_policy"]+"/"+virtual_folder+"/"+obj )
                
                #return Response(headers=resp.headers,request=req)
                return HTTPUnauthorized('Done!\n')
        
            if req.method == 'GET':
                
                if not cc.verify_access(self, req.environ, version, account, container, virtual_folder): # Chack access to virtual folder
                    return HTTPUnauthorized('Folder error: Perhaps '+account+"/"+container+"/"+virtual_folder+' doesn\'t exists in Swift.')
                                
                file_path, success = cc.verify_virtual_folder_file_access(self, req.environ, version, account, container, virtual_folder, obj)
                
                if not success:
                    return HTTPUnauthorized('Object error: Perhaps '+obj+' doesn\'t exists in Swift.')
 
                resp = cc.get_file(self, req.environ, version, account, file_path)

                if not resp:
                    return HTTPUnauthorized('Object error: Perhaps '+file_path+' doesn\'t exists in Swift.\n')
               
                return resp
                   
        """    
        """
        ########### OBJECT SERVER CASE: PRE-PROCESSING -> PUT METADATA FILE
        """

        if self.execution_server == 'object' and req.method == 'PUT' and ( any((True for x in self.available_triggers if x in req.headers.keys())) ):
            
            header = [i for i in self.available_triggers if i in req.headers.keys()]
            if len(header) > 1:
                return HTTPUnauthorized('The system can only set 1 controller each time.\n')
            handler = req.headers[header[0]]
             
            # Get physical location of main file
            
            get_req = req.copy_get()
            get_resp = get_req.get_response(self.app)
            file_path = get_resp.app_iter._data_file.rsplit('/', 1)[0]
            self.logger.info('Swift Controller - File path: '+file_path)
            
            #write handler to file metadata
            docker_gateway = cdg.ControllerGatewayDocker(req, get_resp, self.hconf, self.logger, self.app, 
                                                         device, partition, account, container, obj)
            
            docker_gateway.setHandler(header[0],handler)
            
            # Write metadata file
            self.logger.info('Swift Controller - File path: '+file_path)
            metadata_target_path = os.path.join(file_path, handler.rsplit('.', 1)[0]+".md")
            fn = open(metadata_target_path, 'w')
            fn.write(req.body)
            fn.close()

            return HTTPUnauthorized('Metadata file saved correctly.\n')
        
        
        """
        ************ OBJECT SERVER CASE: PRE-PROCESSING -> VIRTUAL FOLDER (WRITE METADATA)
        """
        """
        if self.execution_server == 'object' and req.method == 'PUT' and "X-Use-Controller" in req.headers:
            
            print "-------------- OBJECT: USE CONTROLLER ----------------------"
            
            # COMPROVAR SI HAY OTRA FORMA DE OBTENER EL PATH
            get_req = req.copy_get()
            get_resp = get_req.get_response(self.app)
            file_path = get_resp.app_iter._data_file.rsplit('/', 1)[0]
            fd = get_resp.app_iter._fp
            #################################################
            
            print file_path
   
            file_md = ast.literal_eval(req.headers["X-Metadata"])
 
            cc.write_metadata(fd, req.headers["X-Metadata"], 65563, "user.swift.controller.file."+file_md["name"])
            
            return Response(headers=get_resp.headers,request=req)    
        """
        
        # -----------------------------------------------------------------------------------------------------------------
        # -----------------------------------------------------------------------------------------------------------------
        orig_resp = req.get_response(self.app)   
        # -----------------------------------------------------------------------------------------------------------------
        # -----------------------------------------------------------------------------------------------------------------

        """
        ################################### PROXY SERVER CASE: POST-PROCESSING ############################################
        """
        
        if self.execution_server == 'proxy' and 'Total-Storlets-To-Execute-On-Proxy' in orig_resp.headers:

            self.logger.info('Swift Controller - There are Storlets to execute from object server micro-controller')
            
            storlet_gateway = csg.ControllerGatewayStorlet(self.hconf, self.logger, self.app, account, container, obj)
            account_meta = storlet_gateway.getAccountInfo()     
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
                
                if not storlet_gateway.authorizeStorletExecution(storlet):
                    return HTTPUnauthorized('Swift Controller - Storlet: No permission')

                old_env = req.environ.copy()
                orig_req = Request.blank(old_env['PATH_INFO'], old_env)
                out_fd, app_iter = storlet_gateway.executeStorletOnProxy(orig_resp, parameters, out_fd)
                
                
             
            old_env = req.environ.copy()
            orig_req = Request.blank(old_env['PATH_INFO'],
                                     old_env)
            resp_headers = orig_resp.headers

            resp_headers['Content-Length'] = None
            
            return Response(app_iter=app_iter,
                            headers=resp_headers,
                            request=orig_req,
                            conditional_response=True)
            
        
        # NO STORTLETS TO EXECUTE ON PROXY 
        elif self.execution_server == 'proxy' and req.method == "GET" and (orig_resp.headers["Storlet-Executed"] or "X-Object-Meta-Run-Micro-Controller" in orig_resp.headers):
            
            self.logger.info('Swift Controller - There are NO Storlets to execute from object server micro-controller')
            
            #COMPROVAR SI ES NECESARIO EJECUTAR EL CONTROLADOR AQUI
            req.headers['X-Current-Server'] = "Proxy"
                        
            if "X-Object-Meta-Run-Micro-Controller" in orig_resp.headers: # We must execute the micro-controller here in the proxy
                micro_controller = orig_resp.headers["X-Object-Meta-Run-Micro-Controller"]
                orig_resp.headers["X-Object-Meta-Path"] = "http://"+req.host+req.path_info
                
                docker_gateway = cdg.ControllerGatewayDocker(req, orig_resp, self.hconf, self.logger, self.app, None, None, account, container, obj)
                
                #RUN MICROCONTROLLER HERE
                docker_gateway.setHandlers(micro_controller)
                
                self.logger.info('Swift Controller - There are micro-controllers to execute')
                
                # We need to start Internal CLient
                docker_gateway.startInternalClient()
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
        
      
         
      
        """
        ################################### OBJECT SERVER CASE: POST-PROCESSING ############################################
        """
        
        """
        # VIRTUAL FOLDER
        if self.execution_server == 'object' and req.method == "GET" and "X-Use-Controller" in req.headers:   
            print "------------------->HEAD<-----------------------"
            obj = req.headers["X-User-Object"]
            fd = orig_resp.app_iter._fp
            
            obj_meta = cc.read_metadata(fd, "user.swift.controller.file."+obj)
            
            if obj_meta:
                obj_meta = ast.literal_eval(obj_meta)
                orig_resp.headers["Requested-file"] = obj_meta["path"]
            
            return orig_resp
        #--------------------------------------------------------------------------------------------------
        """
                   
        if self.execution_server == 'object':
            
            #print "----------------------------------------------------"
            #print orig_resp.app_iter
            #print "----------------------------------------------------"
        
            
            self.logger.info('Swift Controller - Object Server execution')
            req.headers['X-Current-Server'] = "Object"
            
            #check if is a valid request
            if not self.valid_request(req, container):
                # We only want to process PUT and GET requests
                # Also we ignore the calls that goes to the storlet, handler and dependency container  
                return orig_resp
            
            
            docker_gateway = cdg.ControllerGatewayDocker(req, orig_resp, self.hconf, self.logger, self.app, 
                                                         device, partition, account, container, obj)
            
            if docker_gateway.getHandlers():
                self.logger.info('Swift Controller - There are micro-controllers to execute')
                
                # We need to start Internal CLient
                #docker_gateway.startInternalClient()
                # We need to start container if it is stopped
                #docker_gateway.startContainer()  # NO SEMPRE

                # Go to run the micro-controller         
                storlet_list = docker_gateway.executeControllerHandlers()     
                
                # Go to run the Storlet whether the microcontroller sends back any.
                if storlet_list: 
                    storlet_gateway = csg.ControllerGatewayStorlet(self.hconf, self.logger, self.app, account, container, obj)
                    account_meta = storlet_gateway.getAccountInfo()     
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
                        
                        self.logger.info('Swift Controller - Go to execute '+storlet+' storlet with parameters "'+parameters+'"'+ " on "+ nodeToExecute)
                        
                        if nodeToExecute == "object-server":
                            if not storlet_gateway.authorizeStorletExecution(storlet):
                                return HTTPUnauthorized('Swift Controller - Storlet: No permission')
    
                            old_env = req.environ.copy()
                            orig_req = Request.blank(old_env['PATH_INFO'], old_env)
                            out_fd, app_iter = storlet_gateway.executeStorlet(orig_resp,parameters, out_fd)
                            
                            # Notify to proxy that Storlet was executed in the object-server
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
            
        return orig_resp

    def valid_request(self, req, container):
        if (req.method == 'GET') and container != self.hconf["storlet_container"] and container != self.hconf["handler_dependency"] and container != self.hconf["handler_container"]:
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
    
    controller_conf = dict()
    controller_conf['execution_server'] = conf.get('execution_server', 'object')
    controller_conf['controller_timeout'] = conf.get('controller_timeout', 20)
    controller_conf['controller_pipe'] = conf.get('controller_pipe', 'controller_pipe')
    controller_conf['handlers_dir'] = conf.get('handlers_dir', '/home/lxc_device/handlers/scopes')
    controller_conf['handler_container'] = conf.get('handler_container', "handler")
    controller_conf['handler_dependency'] = conf.get('handler_container', "dependency")
    
    controller_conf['storlet_timeout'] = conf.get('storlet_timeout', 40) 
    controller_conf['storlet_container'] = conf.get('storlet_container', "storlet")
    controller_conf['storlet_dependency'] = conf.get('storlet_dependency', "dependency")
    
    # Virtual Folders
    controller_conf["default_policy"] = conf.get('default_policy', "1X")
    controller_conf["root_container"] = conf.get('root_container', "CONTROLLER-ROOT")
    
    controller_conf['docker_repo'] = conf.get('docker_repo', "10.30.239.240:5001")
       
    configParser = ConfigParser.RawConfigParser()
    configParser.read(conf.get('storlet_gateway_conf', '/etc/swift/storlet_docker_gateway.conf'))
    
    additional_items = configParser.items("DEFAULT")
    for key, val in additional_items:
        controller_conf[key] = val

    def swift_controller(app):
        return SwiftControllerMiddleware(app, controller_conf)

    return swift_controller

