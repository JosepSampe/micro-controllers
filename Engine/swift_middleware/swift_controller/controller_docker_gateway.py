'''===========================================================================
15-Oct-2015    josep.sampe    Initial implementation.
==========================================================================='''
from SBusPythonFacade.SBus import SBus
from SBusPythonFacade.SBusDatagram import SBusDatagram

from eventlet.timeout import Timeout
import controller_common as cc

import os
import select
import json
import subprocess
import time

SBUS_FD_INPUT_OBJECT = 0
SBUS_FD_OUTPUT_OBJECT = 1
SBUS_FD_OUTPUT_OBJECT_METADATA = 2
SBUS_FD_LOGGER = 4

SBUS_CMD_EXECUTE = 1


class ControllerGatewayDocker():

    def __init__(self, req, orig_resp, hconf, logger, app, device, partition, account, container, obj):
        self.req = req
        self.orig_resp = orig_resp
        self.hconf = hconf
        self.logger = logger
        self.app = app
        self.version = 0
        self.device = device
        self.partition = partition      
        self.account = account
        self.container = container
        self.obj = obj
        self.scope = account[5:18]
        self.controller_timeout = hconf["controller_timeout"]
        self.file_path = None
        self.handler_metadata = dict()
        
        #CONTAINER
        self.docker_image_name_prefix = "controller"
        self.docker_repo = hconf['docker_repo']

                 
                 
    def startInternalClient(self):
        self.logger.info('Swift Controller - Going to execute Internal Client')
 
        pid = os.popen( "ps -aef | grep -i 'InternalClient.py' | grep -v 'grep' | awk '{ print $2 }'" ).read()

        if pid != "":
            self.logger.info('Swift Controller - Internal Client is already started')
        else:
            #TODO: Change IC path
            cmd='/usr/bin/python /home/lab144/josep/ControllerMiddleware/InternalClient.py' \
                '/home/lxc_device/pipes/scopes/bd34c4073b654/internal_client_pipe DEBUG &'
                  
            self.logger.info(cmd)
            p = subprocess.call(cmd,shell=True)
                    
            if p == 0:
                self.logger.info('Swift Controller - Internal Client started')   
            else:
                self.logger.info('Swift Controller - Error starting Internal Client')
            
            time.sleep(1)
        
        
        
    def startContainer(self):

        # Extract the account's ID from the account
        if self.account.lower().startswith('auth_'):
            account_id = self.account[len('auth_'):]
        else:
            account_id = self.account
            
        docker_container_name = '%s_%s' % (self.docker_image_name_prefix,account_id)
        docker_image_name = '%s/%s' % (self.docker_repo, account_id)
             
        self.host_pipe_prefix =  self.hconf["pipes_dir"]+"/"+self.scope
        self.sandbox_pipe_prefix = "/mnt/channels"
        
        pipe_mount = '%s:%s' % (self.host_pipe_prefix, self.sandbox_pipe_prefix)
        
        
        self.host_storlet_prefix = self.hconf["handlers_dir"]+"/"+self.scope
        self.sandbox_storlet_dir_prefix = "/home/swift"
        
        controller_mount = '%s:%s' % (self.host_storlet_prefix, self.sandbox_storlet_dir_prefix)

        cmd = "sudo docker run --net=none --name "+docker_container_name+" -d -v /dev/log:/dev/log -v " \
              +pipe_mount+" -v "+controller_mount+" -i -t "+docker_image_name+" debug /home/swift/start_daemon.sh"

        self.logger.info(cmd)
        
        self.logger.info('Swift Controller - Starting container '+docker_container_name+'...')
               
        p = subprocess.call(cmd,shell=True)
        
        if p == 0:
            time.sleep(1)
            self.logger.info('Swift Controller - Container '+docker_container_name+' started')   
        else:
            self.logger.info('Swift Controller - Container '+docker_container_name+' is already started')

 
    def setHandler(self,trigger,handler):     
        trigger = trigger.rsplit('-', 1)[1].lower()
        fd = self.orig_resp.app_iter._fp
        controller_md = cc.read_metadata(fd)
        
        if not controller_md:
            controller_md = {'onget': 'None', 'onput': 'None', 'ondelete': 'None', 'ontimer': 'None'}

        controller_md[trigger] = handler
        cc.write_metadata(fd,controller_md)


    def setHandlers(self, hadlers):
        self.handler_list = hadlers.split(",")

    def getHandlers(self):
    
        req = self.orig_resp.environ["REQUEST_METHOD"]        
        fd = self.orig_resp.app_iter._fp
        
        controller_md = cc.read_metadata(fd)

        if controller_md:
            self.handler_list = controller_md["on"+req.lower()].split(",")
            if self.handler_list == 'None':
                return False
            return True
        else:
            return False

        
        
    def executeControllerHandlers(self, server = None):
        
        if server == "proxy":
            self.file_path = "/tmp/"
        else:
            self.file_path = self.orig_resp.app_iter._data_file.rsplit('/', 1)[0]
            
               
        #verify access to handlers and dependencies, and update cache
        for handler in self.handler_list:
            hverified = self.verify_access(self.hconf["handler_container"], handler)
            #"""
            if hverified:
                self.update_handler_cache("handler",handler,handler)
                for dependency in self.handler_metadata[handler]["X-Object-Meta-Handler-Library-Dependency"].split(","):
                    dverified = self.verify_access(self.hconf["handler_dependency"],dependency)
                    if dverified:
                        self.update_handler_cache("dependency",handler,dependency)
                    else:
                        self.logger.error('Swift Controller - Dependency '+dependency+" not found in Swift")
                        raise NameError("Swift Controller - Dependency "+dependency+" not found in Swift")
                    
            else:
                raise NameError("Swift Controller - Handler "+handler+" not found in Swift")
            #"""       
        
        
        controller_logger_path = self.hconf["log_dir"]+"/"+self.scope+"/"
        controller_pipe_path = self.hconf["pipes_dir"]+"/"+self.scope+"/"+self.hconf["controller_pipe"]

        self.logger.info('Swift Controller - File path: '+self.file_path)
        
        protocol = HandlerInvocationProtocol(self.file_path, controller_pipe_path, controller_logger_path, 
                                             dict(self.req.headers), self.orig_resp.headers, self.handler_list, 
                                             self.handler_metadata, self.controller_timeout, self.logger)
               
        out_data = protocol.communicate()
        
        
        #self.logger.info('Swift Controller - Micro-Controller Out: '+json.dumps(out_data))
        
        return out_data
                
    def verify_access(self, container, handler):
        resp = cc.makeSwiftRequest("HEAD", self.account, container, handler)

        if resp.status_int < 300 and resp.status_int >= 200:
            if container == self.hconf["handler_container"]:
                self.handler_metadata[handler] = resp.headers
                #print self.handler_metadata[handler]
            return True

        return False   

    def update_handler_cache(self, container,handler, obj):   
        resp = cc.makeSwiftRequest("GET", self.account, container, obj)

        docker_handler_path = self.hconf["handlers_dir"]+"/"+self.scope+"/"+self.handler_metadata[handler]['X-Object-Meta-Handler-Main']
        
        #print docker_handler_path
        docker_target_path = os.path.join(docker_handler_path, obj)
        if not os.path.exists(docker_handler_path):
            os.makedirs(docker_handler_path, 0o755)

        fn = open(docker_target_path, 'w')
        fn.write(resp.body)
        fn.close()


class HandlerInvocationProtocol(object):
    
    def __init__(self, file_path, controller_pipe_path, controller_logger_path, req_haders, 
                 file_headers, handlers_list, handler_metadata, timeout, logger):
        self.logger = logger
        self.controller_pipe_path = controller_pipe_path
        self.controller_logger_path = controller_logger_path
        self.timeout = timeout
        self.req_md = req_haders
        self.file_md = file_headers
        self.handlers_list = handlers_list # Handler name list
        self.handler_metadata = handler_metadata #Handler metadata
        self.file_path = file_path # Path of requested object
        self.handlers = list() # Handler object list

        # remote side file descriptors and their metadata lists
        # to be sent as part of invocation
        self.fds = list()
        self.fdmd = list()

        # local side file descriptors      
        self.response_read_fd = None
        self.response_write_fd = None
        self.null_read_fd = None
        self.null_write_fd = None
        self.task_id = None
        
    def _add_output_stream(self):
        self.fds.append(self.response_write_fd)
        md = dict()
        md['type'] = SBUS_FD_OUTPUT_OBJECT
        self.fdmd.append(md)
        
    def _add_logger_stream(self):
        for handler in self.handlers:
            self.fds.append(handler.getlogfd())
            md = dict()
            md['type'] = SBUS_FD_LOGGER
            md['handler'] = handler.getname()
            #print md
            self.fdmd.append(md)
        
    def _add_metadata_stream(self): ##ADDED
        for handler in self.handlers: 
            self.fds.append(handler.getmdfd())
            md = dict()
            md['type'] = SBUS_FD_OUTPUT_OBJECT_METADATA
            md['handler'] = handler.getname()
            md['main'] = handler.getmain()
            md['dependencies'] = handler.getdependencies()
            #print md
            self.fdmd.append(md)
            
    def _add_file_req_md(self):
        self.fds.append(self.null_write_fd)
        if "X-Service-Catalog" in self.req_md:
            del self.req_md['X-Service-Catalog']
        
                  
        if "Cookie" in self.req_md:   
            del self.req_md['Cookie']
        
        
        headers = {'req_md':self.req_md,'file_md':self.file_md}

        
        md = dict()
        md['type'] = SBUS_FD_INPUT_OBJECT
        md['json_md'] = json.dumps(headers)
        #print md
        self.fdmd.append(md)

    def _prepare_invocation_descriptors(self):
        # Add the response stream
        self.response_read_fd, self.response_write_fd = os.pipe()
        self.null_read_fd, self.null_write_fd = os.pipe()

        # Add req and file headers
        self._add_file_req_md()   
        # Add output pipe
        self._add_output_stream()  
        # Add the loggers
        self._add_logger_stream()
        # Add the metadata files
        self._add_metadata_stream()
 

    def _close_remote_side_descriptors(self):
        if self.response_write_fd:
            os.close(self.response_write_fd)

    def _invoke(self):
                
        dtg = SBusDatagram()
        dtg.set_files(self.fds)
        dtg.set_metadata(self.fdmd)
        #dtg.set_exec_params(prms)
        dtg.set_command(SBUS_CMD_EXECUTE)

        rc = SBus.send(self.controller_pipe_path, dtg) #Send datagram to container daemon
        if (rc < 0):
            raise Exception("Failed to send execute command")
    
    def _wait_for_read_with_timeout(self, fd):
        r, w, e = select.select([fd], [], [], self.timeout)
        if len(r) == 0:
            if self.task_id:
                self._cancel()
            raise Timeout('Timeout while waiting for handler output')
        if fd in r:
            return

    def _read_response(self):
        self._wait_for_read_with_timeout(self.response_read_fd)
        flat_json = os.read(self.response_read_fd, 1024)
        if flat_json is not None:
            out_data = json.loads(flat_json)
        return out_data

    def communicate(self):
        for handler_name in self.handlers_list:           
            self.handlers.append(Handler(self.file_path, self.controller_logger_path, handler_name, 
                                         self.handler_metadata[handler_name]['X-Object-Meta-Handler-Main'],
                                         self.handler_metadata[handler_name]["X-Object-Meta-Handler-Library-Dependency"]))
        
        for handler in self.handlers:
            handler.open()
        
        self._prepare_invocation_descriptors()
        
        try:
            self._invoke()
        except Exception as e:
            raise e
        finally:
            self._close_remote_side_descriptors()
            for handler in self.handlers:
                handler.close()

        out_data = self._read_response()
        os.close(self.response_read_fd)
                
        return out_data

       
class Handler(object):
    
    def __init__(self, file_path, logger_path, name, main, dependencies):

        self.full_md_path = os.path.join(file_path, '%s.md' % name.rsplit('.', 1)[0])
        self.full_log_path = os.path.join(logger_path, '%s/%s.log' % (main,  name.rsplit('.', 1)[0]))
        self.handler = name
        self.main_class = main
        self.dependencies = dependencies
        
        if not os.path.exists(os.path.join(logger_path, '%s' % main)):
            os.makedirs(os.path.join(logger_path, '%s' % main))
            
    def open(self):        
        self.metadata_file = open(self.full_md_path, 'a+')
        self.logger_file = open(self.full_log_path, 'a')

    def getmdfd(self):
        return self.metadata_file.fileno()
    
    def getlogfd(self):
        return self.logger_file.fileno()

    def getname(self):
        return self.handler

    def getdependencies(self):
        return self.dependencies

    def getmain(self):
        return self.main_class

    def getsize(self):
        statinfo = os.stat(self.full_path)
        return statinfo.st_size

    def close(self):
        self.metadata_file.close()
        self.logger_file.close()
