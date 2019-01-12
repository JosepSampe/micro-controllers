from eventlet.timeout import Timeout
from vertigo_middleware.gateways.docker.bus import Bus
from vertigo_middleware.gateways.docker.datagram import Datagram
import select
import json
import os
import subprocess
import time
import cmd

SBUS_FD_INPUT_OBJECT = 0
SBUS_FD_OUTPUT_OBJECT = 1
SBUS_FD_OUTPUT_OBJECT_METADATA = 2
SBUS_FD_LOGGER = 4

SBUS_CMD_EXECUTE = 1

MC_MAIN_HEADER = "X-Object-Meta-Microcontroller-Main"
MC_DEP_HEADER = "X-Object-Meta-Microcontroller-Library-Dependency"


class RunTimeSandbox(object):
    """
    The RunTimeSandbox represents a re-usable per scope sandbox.
    """

    def __init__(self, logger, conf, account):
        self.scope = account[5:18]
        self.conf = conf
        self.logger = logger
        self.docker_img_prefix = 'vertigo'
        self.docker_repo = conf['docker_repo']

    def _is_started(self, container_name):
        """
        Auxiliary function that checks whether the container is started.

        :param docker_container_name : name of the container
        :returns: whether exists
        """
        cmd = ("docker ps | grep -v 'grep' | grep '" +
               container_name + "' | awk '{print $1}'")
        docker_id = os.popen(cmd).read()

        if not docker_id:
            return False

        return True

    def _is_stopped(self, container_name):
        """
        Auxiliary function that checks whether the container is started.

        :param docker_container_name : name of the container
        :returns: whether exists
        """
        cmd = ("docker ps -f 'status=exited' | grep -v 'grep' | grep '" +
               container_name + "' | awk '{print $1}'")
        docker_id = os.popen(cmd).read()

        if not docker_id:
            return False

        return True

    def _delete(self, container_name):
        cmd = ("docker rm -f " + container_name)
        os.popen(cmd)

    def start(self):
        """
        Starts the docker container.
        """
        container_name = '%s_%s' % (self.docker_img_prefix, self.scope)

        if self._is_stopped(container_name):
            self._delete(container_name)

        if not self._is_started(container_name):
            if self.docker_repo:
                docker_image_name = '%s/%s' % (self.docker_repo, self.scope)
            else:
                docker_image_name = self.scope

            host_pipe_prefix = self.conf["pipes_dir"] + "/" + self.scope
            sandbox_pipe_prefix = "/mnt/channels"

            pipe_mount = '%s:%s' % (host_pipe_prefix, sandbox_pipe_prefix)

            host_storlet_prefix = self.conf["mc_dir"] + "/" + self.scope
            sandbox_storlet_dir_prefix = "/home/swift"

            mc_mount = '%s:%s' % (host_storlet_prefix,
                                  sandbox_storlet_dir_prefix)

            cmd = "docker run --name " + container_name + \
                  " -d -v /dev/log:/dev/log -v " + pipe_mount + \
                  " -v " + mc_mount + " -i -t " + docker_image_name + \
                  " debug /home/swift/start_daemon.sh"

            self.logger.info('Vertigo - Starting container ' +
                             container_name + ' ...')
            self.logger.info(cmd)

            p = subprocess.call(cmd, shell=True)

            if p == 0:
                time.sleep(1)
                self.logger.info('Vertigo - Container "' +
                                 container_name + '" started')
        else:
            self.logger.info('Vertigo - Container "' +
                             container_name + '" is already started')


class MicroController(object):
    """
    Microcontroller main class.
    """

    def __init__(self, logger_path, name, main, dependencies):
        self.log_path = os.path.join(logger_path, main)
        self.log_name = name.replace('jar', 'log')
        self.full_log_path = os.path.join(self.log_path, self.log_name)
        self.micro_controller = name
        self.main_class = main
        self.dependencies = dependencies

        if not os.path.exists(self.log_path):
            os.makedirs(self.log_path)

    def open(self):
        self.logger_file = open(self.full_log_path, 'a')

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
        self.logger_file.close()


class VertigoInvocationProtocol(object):

    def __init__(self, mc_pipe_path, mc_logger_path, req_headers,
                 object_headers, mc_list, mc_metadata, timeout, logger):
        self.logger = logger
        self.mc_pipe_path = mc_pipe_path
        self.mc_logger_path = mc_logger_path
        self.timeout = timeout
        self.req_md = req_headers
        self.object_md = object_headers
        self.mc_list = mc_list  # Ordered microcontroller execution list
        self.mc_md = mc_metadata  # Microcontroller metadata
        self.microcontrollers = list()  # Microcontroller object list

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
        for mc in self.microcontrollers:
            self.fds.append(mc.get_logfd())
            md = dict()
            md['type'] = SBUS_FD_LOGGER
            md['microcontroller'] = mc.get_name()
            md['main'] = mc.get_main()
            md['dependencies'] = mc.get_dependencies()
            self.fdmd.append(md)

    def _add_object_req_md(self):
        self.fds.append(self.null_write_fd)
        if "X-Service-Catalog" in self.req_md:
            del self.req_md['X-Service-Catalog']

        if "Cookie" in self.req_md:
            del self.req_md['Cookie']

        headers = {'req_md': self.req_md, 'object_md': self.object_md}

        md = dict()
        md['type'] = SBUS_FD_INPUT_OBJECT
        md['json_md'] = json.dumps(headers)
        self.fdmd.append(md)

    def _prepare_invocation_descriptors(self):
        # Add the response stream
        self.response_read_fd, self.response_write_fd = os.pipe()
        self.null_read_fd, self.null_write_fd = os.pipe()

        # Add req and file headers
        self._add_object_req_md()
        # Add output pipe
        self._add_output_stream()
        # Add the loggers
        self._add_logger_stream()

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
            raise Timeout('Timeout while waiting for Microcontroller output')
        if fd in r:
            return

    def _read_response(self):
        mc_response = dict()
        for mc_name in self.mc_list:
            self._wait_for_read_with_timeout(self.response_read_fd)
            flat_json = os.read(self.response_read_fd, 1024)

            if flat_json:
                mc_response[mc_name] = json.loads(flat_json)
            else:
                mc_response[mc_name] = dict()
                mc_response[mc_name]['command'] = 'CANCEL'
                mc_response[mc_name]['message'] = ('Error running ' + mc_name +
                                                   ': No response from microcontroller.')

        out_data = dict()
        for mc_name in self.mc_list:
            command = mc_response[mc_name]['command']
            if command == 'CANCEL':
                out_data['command'] = command
                out_data['message'] = mc_response[mc_name]['message']
                break
            if command == 'REWIRE':
                out_data['command'] = command
                out_data['object_id'] = mc_response[mc_name]['object_id']
                break
            if command == 'STORLET':
                out_data['command'] = command
                if 'list' not in out_data:
                    out_data['list'] = dict()
                for k in sorted(mc_response[mc_name]['list']):
                    new_key = len(out_data['list'])
                    out_data['list'][new_key] = mc_response[mc_name]['list'][k]
            if command == 'CONTINUE':
                if 'command' not in out_data:
                    out_data['command'] = command

        return out_data

    def communicate(self):
        for mc_name in self.mc_list:
            mc = MicroController(self.mc_logger_path,
                                 mc_name,
                                 self.mc_md[mc_name][MC_MAIN_HEADER],
                                 self.mc_md[mc_name][MC_DEP_HEADER])
            self.microcontrollers.append(mc)

        for mc in self.microcontrollers:
            mc.open()

        self._prepare_invocation_descriptors()

        try:
            self._invoke()
        except Exception as e:
            raise e
        finally:
            self._close_remote_side_descriptors()
            for mc in self.microcontrollers:
                mc.close()

        out_data = self._read_response()
        os.close(self.response_read_fd)

        return out_data
