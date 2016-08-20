import subprocess
import os

class Api(object):
    """
    The RunTimeSandbox represents a re-usable per scope sandbox.
    """
    def __init__(self, logger, conf, account):
        """
        :param logger: swift.common.utils.LogAdapter instance
        :param pipe_path: path where the pipe is located for the current tenant
        """
        self.logger = logger
        self.conf = conf
        self.account = account
        self.scope = account[5:18]
        
        self.pipe_path = os.path.join(conf["pipes_dir"], self.scope)
        self.api_pipe_path = os.path.join(self.pipe_path , conf["api_pipe"])

    @property
    def is_started(self):
        """
        Auxiliary function that checks whether the local API is started.
        
        :param docker_container_name : name of the container
        :returns: whether exists
        """ 
        cmd = ("ps -aef | grep -i 'api_daemon.py' | grep " +
               "-v 'grep' | grep '"+self.api_pipe_path+"'| awk '{ print $2 }'")
        
        pid = os.popen(cmd).read()
        
        if not pid:
            return False

        return True
        
    def start(self):
        """
        Starts the internal client daemon. This method starts one IC for
        each tenant. If the internal API is already started, it does nothing.
        """                   
        if not self.is_started:
            cmd = '/usr/bin/python /opt/vertigo/api_daemon.py ' \
                  + self.account +' '+self.api_pipe_path +' DEBUG &'
    
            self.logger.info(cmd)
            # TODO: Is better to call an external script?
            p = subprocess.call(cmd, shell=True)
    
            if p == 0:
                self.logger.info('Vertigo - Local API daemon started')
            else:
                self.logger.info('Vertigo - Error starting local API')
        else:
            self.logger.info('Vertigo - Local API is already started')
