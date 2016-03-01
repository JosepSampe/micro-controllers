import sys
import logging
import time
import pwd
import os
import pickle
from logging.handlers import SysLogHandler
from SBusPythonFacade.SBus import SBus
from swiftclient import client as ic
from hashlib import md5
import memcache

PICKLE_PROTOCOL = 2

def md5hash(key):
    return md5(key).hexdigest()

class internal_client(object):
    
    def __init__(self, path, logger):
        '''@summary:             CTOR
                              Prepare the auxiliary data structures
        @param path:          Path to the pipe file internal SBus listens to
        @type  path:          String
        @param logger:        Logger to dump 0the information to
        @type  logger:        SysLogHandler
        '''
        self.logger = logger
        self.pipe_path = path
        
        # Dictionary: map storlet name to pipe name
        self.storlet_name_to_pipe_name = dict()
        # Dictionary: map storlet name to daemon process PID
        self.storlet_name_to_pid = dict()
        # Strat route to memcahced
        self.mc = memcache.Client(['controller:11211'], debug=0)
    '''--------------------------------------------------------------------'''
       
    def _copy_file(self, token, source_file, dest_file, source_type):      
        self.logger.debug('Going to copy '+source_file+" to "+dest_file)

        url = source_file.rsplit("/",2)[0]
        account = url.rsplit("/",1)[1]
        container = source_file.rsplit("/",2)[1]
        name = source_file.rsplit("/",2)[2]
        
        dest_container = dest_file.split("/")[0]
        dest_name = dest_file.split("/")[1]
        
        response = dict() 
        if source_type == "original":
            headers = {'X-Copy-From':'/'+container+'/'+name}
            ic.put_object(url=url, token=token, 
                          container=dest_container, 
                          name=dest_name, 
                          headers=headers, 
                          response_dict=response)
      
        else: #PROCESSED BY STROELTS: EXECUTE ON PROXY
            headers = {'X-Copy-File':'True'}
            data = ic.get_object(url=url, token=token, 
                                 container=dest_container, 
                                 name=dest_name, 
                                 headers=headers, 
                                 response_dict=response)
            
            #Copy to another Container?                     
            #ic.put_object(url, token, dest_container, dest_name, data[1], None, None, None, None, headers, None, None, None, response)

            # Store the processed object in cache
            key = md5hash(account+"/"+container+"/"+name)
            self.mc.set(key, data[1])

    def _delete_object(self, token, source_file):
        self.logger.debug('Going to delete '+source_file)
        response = dict()
        url = source_file.rsplit("/",2)[0]
        container = source_file.rsplit("/",2)[1]
        name = source_file.rsplit("/",2)[2]
        
        ic.delete_object(url=url, token=token, 
                         container=container, 
                         name=name, 
                         response_dict=response)
        
    def _prefetch_object(self, token, original_object_path, source_files_list):
        self.logger.debug('Going to prefetch '+source_files_list)
        
        headers = {'X-Copy-File':'True'}
        response = dict() 
          
        original_object_path = original_object_path.split(" ")[1]        
        url = original_object_path.rsplit("/",2)[0]
        account = url.rsplit("/",1)[1]
        container = original_object_path.rsplit("/",2)[1]
        file_list = source_files_list.split(",")

        swift_conn = ic.Connection(preauthtoken=token, preauthurl=url)

        for obj in file_list:
            key = md5hash(account+"/"+container+"/"+obj)
            self.mc.delete(key)
            
            data = swift_conn.get_object(container=container, 
                                         obj=obj, 
                                         headers=headers, 
                                         response_dict=response)
            #PREFETCH        
            object_data = dict()
            object_data["headers"] = data[0]
            object_data["body"] = data[1]
                        
            value = pickle.dumps(object_data, 2)
            self.mc.set(key, value)

        swift_conn.close()
        
    def dispatch_command(self, dtg):
        command = -1
                
        try:
            command = dtg.get_command()
        except Exception:
            error_text = "Received message does not have command" \
                         " identifier. continuing."
            self.logger.error(error_text)
        else:
            self.logger.debug("Received command {0}".format(command))

        prms = dtg.get_exec_params()
        
        if command == "SBUS_CMD_EXECUTE":
            self.logger.debug('Do SBUS_CMD_EXECUTE')
            self.logger.debug('prms = %s' % str(prms))
      
            swift_command = prms['op']
            
            #time.sleep(3)
            
            if swift_command == "DELETE":
                self._delete_object(prms['swift_token'],prms['source_file'])
                
            if swift_command == "COPY":
                self._copy_file(prms['swift_token'],prms['source_file'],prms['dest_file'],prms['source_type'])
                
            if swift_command == "PREFETCH":
                self._prefetch_object(prms['swift_token'],prms['object_path'],prms['source_file_list'])
            
        
    def main_loop(self):
        '''@summary: main_loop
                  The 'internal' loop. Listen to SBus, receive datagram,
                  dispatch command, report back.
        '''

        # Create SBus. Listen and process requests
        sbus = SBus()
        fd = sbus.create(self.pipe_path)
        if fd < 0:
            self.logger.error("Failed to create SBus. exiting.")
            return

        b_iterate = True

        while b_iterate:
            rc = sbus.listen(fd)
           
            if rc < 0:
                self.logger.error("Failed to wait on SBus. exiting.")
                return
            self.logger.debug("Wait returned")

            dtg = sbus.receive(fd)
            if not dtg:
                self.logger.error("Failed to receive message. exiting.")
                return

            self.dispatch_command(dtg)

        # We left the main loop for some reason. Terminating.
        self.logger.debug('Leaving main loop')

'''------------------------------------------------------------------------'''


def start_logger(logger_name, log_level):
    '''@summary:           start_logger
                        Initialize logging of this process.
                        Set the logger format.
    @param logger_name: The name to report with
    @type  logger_name: String
    @param log_level:   The verbosity level
    @type  log_level:   String
    @rtype:             void
    '''
    logging.raiseExceptions = False
    log_level = log_level.upper()

    if (log_level == 'DEBUG'):
        level = logging.DEBUG
    elif (log_level == 'INFO'):
        level = logging.INFO
    elif (log_level == 'WARNING'):
        level = logging.WARN
    elif (log_level == 'CRITICAL'):
        level = logging.CRITICAL
    else:
        level = logging.ERROR

    logger = logging.getLogger(logger_name)

    if log_level == 'OFF':
        logging.disable(logging.CRITICAL)
    else:
        logger.setLevel(level)

    for i in range(0, 4):
        try:
            sysLogh = SysLogHandler('/dev/log')
            break
        except Exception as e:
            if i < 3:
                time.sleep(1)
            else:
                raise e

    str_format = '%(name)-12s: %(levelname)-8s %(funcName)s' + \
                 ' %(lineno)s [%(process)d, %(threadName)s]' + \
                 ' %(message)s'
    formatter = logging.Formatter(str_format)
    sysLogh.setFormatter(formatter)
    sysLogh.setLevel(level)
    logger.addHandler(sysLogh)
    return logger

'''------------------------------------------------------------------------'''



def usage():
    '''@summary: usage
              Print the expected command line arguments.
    @rtype:   void
    '''
    print("internal_client_daemon <path> <log level>")

'''------------------------------------------------------------------------'''


def main(argv):
    '''@summary: main
              The entry point.
              - Initialize logger,
              - impersonate to swift user,
              - create an instance of daemon_factory,
              - start the main loop.
    '''

    if (len(argv) != 2):
        usage()
        return

    pipe_path = argv[0]
    log_level = argv[1]
    # container_id = argv[2]
    logger = start_logger("Micro-controller Framework Internal Client", log_level)
    logger.debug("Internal client daemon started")
    SBus.start_logger("DEBUG", container_id="InternalClient")

    # Impersonate the swift user
    pw = pwd.getpwnam('swift')
    os.setresgid(pw.pw_gid, pw.pw_gid, pw.pw_gid)
    os.setresuid(pw.pw_uid, pw.pw_uid, pw.pw_uid)

    factory = internal_client(pipe_path, logger)
    factory.main_loop()

'''------------------------------------------------------------------------'''
if __name__ == "__main__":
    main(sys.argv[1:])

'''============================ END OF FILE ==============================='''