#from SBusPythonFacade.SBus import SBus
#from SBusPythonFacade.SBusDatagram import SBusDatagram

from eventlet.timeout import Timeout
from vertigo_common import make_swift_request, get_data_dir, set_swift_metadata, get_swift_metadata
from shutil import copy2
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

MC_MAIN_HEADER = "X-Object-Meta-Handler-Main"
MC_DEP_HEADER = "X-Object-Meta-Handler-Library-Dependency"


class VertigoGatewayDocker():

    def __init__(self, request, response, conf, logger, app, account):
        self.request = request
        self.response = response
        self.conf = conf
        self.logger = logger
        self.app = app
        self.account = account
        self.method = self.request.method.lower()
        self.scope = account[5:18]
        self.execution_server = self.conf["execution_server"]
        self.mc_timeout = self.conf["mc_timeout"]
        self.mc_container = self.conf["mc_container"]
        self.dep_container = self.conf["mc_dependency"]

        # CONTAINER
        self.docker_img_prefix = "vertigo"
        self.docker_repo = conf['docker_repo']

    
    def start_internal_client_daemon(self):
        self.logger.info('Vertigo - Starting Internal Client ...')

        pid = os.popen("ps -aef | grep -i 'internal_client_daemon.py' | grep" +
                       " -v 'grep' | awk '{ print $2 }'").read()

        if pid != "":
            self.logger.info('Vertigo - Internal Client is already' +
                             ' started')
        else:
            # TODO: Change IC path
            cmd = '/usr/bin/python /opt/urv/internal_client_daemon.py ' \
                '/home/lxc_device/pipes/scopes/bd34c4073b654/internal_client_pipe DEBUG &'

            self.logger.info(cmd)

            # TODO: Call external script
            p = subprocess.call(cmd, shell=True)

            print p

            if p == 0:
                self.logger.info('Vertigo - Internal Client daemon' +
                                 ' started')
            else:
                self.logger.info('Vertigo - Error starting Internal' +
                                 ' Client daemon')

            time.sleep(1)

    def start_container(self):

        # Extract the account's ID from the account
        if self.account.lower().startswith('auth_'):
            account_id = self.account[len('auth_'):]
        else:
            account_id = self.account

        docker_container_name = '%s_%s' % (self.docker_img_prefix, account_id)
        docker_image_name = '%s/%s' % (self.docker_repo, account_id)

        host_pipe_prefix = self.conf["pipes_dir"] + "/" + self.scope
        sandbox_pipe_prefix = "/mnt/channels"

        pipe_mount = '%s:%s' % (host_pipe_prefix, sandbox_pipe_prefix)

        host_storlet_prefix = self.conf["mc_dir"] + "/" + self.scope
        sandbox_storlet_dir_prefix = "/home/swift"

        mc_mount = '%s:%s' % (host_storlet_prefix,
                              sandbox_storlet_dir_prefix)

        cmd = "sudo docker run --net=none --name " + docker_container_name + \
              " -d -v /dev/log:/dev/log -v " + pipe_mount + " -v " + mc_mount + \
              " -i -t " + docker_image_name + " debug /home/swift/start_daemon.sh"

        # self.logger.info(cmd)

        self.logger.info('Vertigo - Starting container '
                         + docker_container_name + ' ...')

        p = subprocess.call(cmd, shell=True)

        if p == 0:
            time.sleep(1)
            self.logger.info('Vertigo - Container ' +
                             docker_container_name + ' started')
        else:
            self.logger.info('Vertigo - Container ' +
                             docker_container_name + ' is already started')
            
            
    def _update_cache(self, swift_container, object_name):
        """
        Updates the local cache of microcontrollers and dependencies
        
        :params container: container name
        :params object_name: Name of the microcontroller or dependency
        """
        cache_target_path = os.path.join(self.conf["cache_dir"], self.scope, 'vertigo', swift_container)
        cache_target_obj = os.path.join(cache_target_path, object_name)
        
        if not os.path.exists(cache_target_path):
            os.makedirs(cache_target_path, 0o777)         
        
        resp = make_swift_request("GET", self.account, swift_container, object_name)

        with open(cache_target_obj, 'w') as fn:
                fn.write(resp.body)
                
        set_swift_metadata(cache_target_obj, resp.headers)

    def _is_avialable_in_cache(self, swift_container, object_name):
        """
        checks whether the microcontroler or the dependency is in cache.
        
        :params swift_container: container name (microcontroller or dependency)
        :params object_name: Name of the microcontroller or dependency
        """        
        cached_target_obj = os.path.join(self.conf["cache_dir"], self.scope, 'vertigo', swift_container, object_name)
        self.logger.info('Vertigo - Checking in cache: ' + swift_container+'/'+object_name)       
        
        if not os.path.isfile(cached_target_obj):
            # If the objects is not in cache, brings it from Swift.
            # TODO(josep): In normal usage, if the object is not in cache, the
            # request fails. The idea is that the cache will be automatically  
            # updated by another service.
            # raise NameError('Vertigo - ' + swift_container+'/'+object_name +' not found in cache.')
            self.logger.info('Vertigo - ' + swift_container+'/'+object_name +' not found in cache.')
            self._update_cache(swift_container, object_name)

        self.logger.info('Vertigo - ' + swift_container+'/'+object_name +' in cache.')
        
        return True
        
    def _update_from_cache(self, mc_main, swift_container, object_name): 
        # if enter to this method means that the objects exist in cache
        cached_target_obj = os.path.join(self.conf["cache_dir"], self.scope, 'vertigo', swift_container, object_name)        
        docker_target_dir = os.path.join(self.conf["mc_dir"], self.scope, mc_main)
        docker_target_obj = os.path.join(docker_target_dir, object_name)
        update_from_cache = False
        
        if not os.path.exists(docker_target_dir):
            os.makedirs(docker_target_dir, 0o777)
            update_from_cache = True
        elif not os.path.isfile(docker_target_obj):
            update_from_cache = True
        else:
            cached_obj_metadata = get_swift_metadata(cached_target_obj)
            docker_obj_metadata = get_swift_metadata(docker_target_obj)
            
            cached_obj_tstamp = float(cached_obj_metadata['X-Timestamp'])
            docker_obj_tstamp = float(docker_obj_metadata['X-Timestamp'])
            
            if cached_obj_tstamp > docker_obj_tstamp:
                update_from_cache = True
        
        if update_from_cache:
            self.logger.info('Vertigo - Going to update from cache: ' + swift_container+'/'+object_name )       
            copy2(cached_target_obj, docker_target_obj)
            metadata = get_swift_metadata(cached_target_obj)
            set_swift_metadata(docker_target_obj, metadata)          
        
    def _get_swift_metadata(self, swift_container, object_name):
        cached_target_obj = os.path.join(self.conf["cache_dir"], self.scope, 
                                         'vertigo', swift_container, object_name)
        metadata = get_swift_metadata(cached_target_obj)
        
        return metadata
        
    def _get_microcontroller_metadata(self, mc_list):
        
        mc_metadata = dict()
                
        for mc_name in mc_list:
            if self._is_avialable_in_cache(self.mc_container, mc_name):
                mc_metadata[mc_name] = self._get_swift_metadata(self.mc_container, mc_name)
                mc_main = mc_metadata[mc_name][MC_MAIN_HEADER]
                self._update_from_cache(mc_main, self.mc_container, mc_name)
                
                dep_list = mc_metadata[mc_name][MC_DEP_HEADER].split(",")
                for dep_name in dep_list: 
                    if self._is_avialable_in_cache(self.dep_container, dep_name): 
                        self._update_from_cache(mc_main, self.dep_container, dep_name)
        
        return mc_metadata
  
    def execute_microcontrollers(self, mc_list):

        # We need to start Internal CLient
        #self.start_internal_client_daemon()  # each tenat their own IC
        # We need to start container if it is stopped
        #self.start_container()  # TODO: NO SEMPRE

        """
        if server == "proxy":
            self.object_path = "/tmp/"
        else:
        """

        mc_metadata = self._get_microcontroller_metadata(mc_list)

        mc_logger_path = self.conf["log_dir"] + "/" + self.scope + "/"
        mc_pipe_path = self.conf["pipes_dir"] + "/" + self.scope + "/" + \
            self.conf["mc_pipe"]

        data_dir = get_data_dir(self)
        self.logger.info('Vertigo - Object path: ' + data_dir)

        self.request.headers['X-Current-Server'] = self.execution_server

        protocol = VertigoInvocationProtocol(data_dir,
                                                     mc_pipe_path,
                                                     mc_logger_path,
                                                     dict(self.request.headers),
                                                     self.response.headers,
                                                     mc_list,
                                                     mc_metadata,
                                                     self.mc_timeout,
                                                     self.logger)

        #return protocol.communicate()


class VertigoInvocationProtocol(object):

    def __init__(self, file_path, mc_pipe_path, mc_logger_path, req_haders,
                 file_headers, mc_list, mc_metadata, timeout, logger):
        self.logger = logger
        self.mc_pipe_path = mc_pipe_path
        self.mc_logger_path = mc_logger_path
        self.timeout = timeout
        self.req_md = req_haders
        self.file_md = file_headers
        self.mc_list = mc_list  # Micro-controller name list
        self.mc_md = mc_metadata  # Micro-controller metadata
        self.object_path = file_path  # Path of requested object
        self.micro_controllers = list()  # Micro-controller object list

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
        for mc in self.micro_controllers:
            self.fds.append(mc.get_logfd())
            md = dict()
            md['type'] = SBUS_FD_LOGGER
            md['handler'] = mc.get_name()
            self.fdmd.append(md)

    def _add_metadata_stream(self):  # ADDED
        for mc in self.micro_controllers:
            self.fds.append(mc.get_mdfd())
            md = dict()
            md['type'] = SBUS_FD_OUTPUT_OBJECT_METADATA
            md['handler'] = mc.get_name()
            md['main'] = mc.get_main()
            md['dependencies'] = mc.get_dependencies()
            self.fdmd.append(md)

    def _add_file_req_md(self):
        self.fds.append(self.null_write_fd)
        if "X-Service-Catalog" in self.req_md:
            del self.req_md['X-Service-Catalog']

        if "Cookie" in self.req_md:
            del self.req_md['Cookie']

        headers = {'req_md': self.req_md, 'file_md': self.file_md}

        md = dict()
        md['type'] = SBUS_FD_INPUT_OBJECT
        md['json_md'] = json.dumps(headers)
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
        # dtg.set_exec_params(prms)
        dtg.set_command(SBUS_CMD_EXECUTE)

        # Send datagram to container daemon
        rc = SBus.send(self.mc_pipe_path, dtg)
        if (rc < 0):
            raise Exception("Failed to send execute command")

    def _wait_for_read_with_timeout(self, fd):
        r, _, _ = select.select([fd], [], [], self.timeout)
        if len(r) == 0:
            if self.task_id:
                self._cancel()
            raise Timeout('Timeout while waiting for Micro-controller output')
        if fd in r:
            return

    def _read_response(self):
        self._wait_for_read_with_timeout(self.response_read_fd)
        flat_json = os.read(self.response_read_fd, 1024)

        if flat_json == "{}":
            out_data = None
        else:
            out_data = json.loads(flat_json)

        return out_data

    def communicate(self):
        for mc_name in self.mc_list:
            mc = MicroController(self.object_path,
                                 self.mc_logger_path,
                                 mc_name,
                                 self.mc_md[mc_name][MC_MAIN_HEADER],
                                 self.mc_md[mc_name][MC_DEP_HEADER])
            self.micro_controllers.append(mc)

        for mc in self.micro_controllers:
            mc.open()

        self._prepare_invocation_descriptors()

        try:
            self._invoke()
        except Exception as e:
            raise e
        finally:
            self._close_remote_side_descriptors()
            for mc in self.micro_controllers:
                mc.close()

        out_data = self._read_response()
        os.close(self.response_read_fd)

        return out_data


class MicroController(object):

    def __init__(self, file_path, logger_path, name, main, dependencies):

        self.full_md_path = os.path.join(file_path, '%s.md' %
                                         name.rsplit('.', 1)[0])
        self.full_log_path = os.path.join(logger_path, '%s/%s.log' %
                                          (main,  name.rsplit('.', 1)[0]))
        self.micro_controller = name
        self.main_class = main
        self.dependencies = dependencies

        if not os.path.exists(os.path.join(logger_path, '%s' % main)):
            os.makedirs(os.path.join(logger_path, '%s' % main))

    def open(self):
        self.metadata_file = open(self.full_md_path, 'a+')
        self.logger_file = open(self.full_log_path, 'a')

    def get_mdfd(self):
        return self.metadata_file.fileno()

    def get_logfd(self):
        return self.logger_file.fileno()

    def get_name(self):
        return self.micro_controller

    def get_dependencies(self):
        return self.dependencies

    def get_main(self):
        return self.main_class

    def get_size(self):
        statinfo = os.stat(self.full_path)
        return statinfo.st_size

    def close(self):
        self.metadata_file.close()
        self.logger_file.close()
