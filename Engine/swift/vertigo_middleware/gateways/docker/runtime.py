from eventlet.timeout import Timeout
from vertigo_middleware.gateways.docker.bus import Bus
from vertigo_middleware.gateways.docker.datagram import Datagram
import select
import json
import os
import subprocess
import time

SBUS_FD_INPUT_OBJECT = 0
SBUS_FD_OUTPUT_OBJECT = 1
SBUS_FD_OUTPUT_OBJECT_METADATA = 2
SBUS_FD_LOGGER = 4

SBUS_CMD_EXECUTE = 1

MC_MAIN_HEADER = "X-Object-Meta-Handler-Main"
MC_DEP_HEADER = "X-Object-Meta-Handler-Library-Dependency"


class RunTimeSandbox(object):
    """
    The RunTimeSandbox represents a re-usable per scope sandbox.
    """
    def __init__(self, conf, logger, account):
        self.account = account[5:]
        self.scope = account[5:18]
        self.conf = conf
        self.logger = logger
        self.docker_img_prefix = 'vertigo'
        self.docker_repo = conf['docker_repo']

    def start(self):
        """
        Starts the docker container.
        """
        docker_container_name = '%s_%s' % (self.docker_img_prefix, self.scope)
        
        
        cmd = "docker ps | grep -v 'grep' | grep '"+docker_container_name+"' | awk '{print $1}'"
        print cmd
        docker_id = os.popen(cmd).read()
        
        print "+++++++++++++", docker_id, "+++++++++++++"
        
        docker_image_name = '%s/%s' % (self.docker_repo, self.account)
    
        host_pipe_prefix = self.conf["pipes_dir"] + "/" + self.scope
        sandbox_pipe_prefix = "/mnt/channels"
    
        pipe_mount = '%s:%s' % (host_pipe_prefix, sandbox_pipe_prefix)
    
        host_storlet_prefix = self.conf["mc_dir"] + "/" + self.scope
        sandbox_storlet_dir_prefix = "/home/swift"
    
        mc_mount = '%s:%s' % (host_storlet_prefix, sandbox_storlet_dir_prefix)
    
        cmd = "sudo docker run --net=none --name " + docker_container_name + \
              " -d -v /dev/log:/dev/log -v " + pipe_mount + " -v " + mc_mount + \
              " -i -t " + docker_image_name + " debug /home/swift/start_daemon.sh"
    
        self.logger.info(cmd)
    
        self.logger.info('Vertigo - Starting container ' + docker_container_name + ' ...')
    
        p = subprocess.call(cmd, shell=True)

        if p == 0:
            time.sleep(1)
            self.logger.info('Vertigo - Container "' + docker_container_name + '" started')
        else:
            self.logger.info('Vertigo - Container "' + docker_container_name + '" is already started')



class MicroController(object):
    """
    Microcontroller main class.
    """
    def __init__(self, object_path, logger_path, name, main, dependencies):

        self.full_md_path = os.path.join(object_path, '%s.md' %
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


class VertigoInvocationProtocol(object):

    def __init__(self, object_path, mc_pipe_path, mc_logger_path, req_haders,
                 file_headers, mc_list, mc_metadata, timeout, logger):
        self.logger = logger
        self.mc_pipe_path = mc_pipe_path
        self.mc_logger_path = mc_logger_path
        self.timeout = timeout
        self.req_md = req_haders
        self.file_md = file_headers
        self.mc_list = mc_list  # Micro-controller name list
        self.mc_md = mc_metadata  # Micro-controller metadata
        self.object_path = object_path  # Path of requested object
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
        dtg = Datagram()
        dtg.set_files(self.fds)
        dtg.set_metadata(self.fdmd)
        # dtg.set_exec_params(prms)
        dtg.set_command(SBUS_CMD_EXECUTE)

        # Send datagram to container daemon
        rc = Bus.send(self.mc_pipe_path, dtg)
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
