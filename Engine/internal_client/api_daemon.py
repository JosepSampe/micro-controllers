from logging.handlers import SysLogHandler
from swiftclient import client as swift
from hashlib import md5
from bus import Bus
import memcache
import logging
import time
import pickle
import sys


STORAGE_URL = "http://192.168.2.1:8080/v1/"

def md5hash(key):
    return md5(key).hexdigest()


class api(object):
    
    def __init__(self, tenant, pipe_path, logger):
        '''
        :param tenant: Tenant ID
        :param pipe_path: Path to the pipe file internal Bus listens to
        :param logger: Logger to dump 0the information to
        '''
        self.tenant = tenant
        self.logger = logger
        self.pipe_path = pipe_path

        self.mc = memcache.Client(['controller:11211'], debug=0)
        self.storage_url = STORAGE_URL + self.tenant

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
            swift.put_object(url=url, token=token, 
                          container=dest_container, 
                          name=dest_name, 
                          headers=headers, 
                          response_dict=response)
      
        else: #PROCESSED BY STROELTS: EXECUTE ON PROXY
            headers = {'X-Copy-File':'True'}
            data = swift.get_object(url=url, token=token, 
                                 container=dest_container, 
                                 name=dest_name, 
                                 headers=headers, 
                                 response_dict=response)
            
            #Copy to another Container?                     
            #ic.put_object(url, token, dest_container, dest_name, data[1], None, None, None, None, headers, None, None, None, response)

            # Store the processed object in cache
            key = md5hash(account+"/"+container+"/"+name)
            self.mc.set(key, data[1])
            
            
    def _prefetch_object(self, token, original_object_path, source_files_list):
        self.logger.debug('Going to prefetch '+source_files_list)
        
        headers = {'X-Copy-File':'True'}
        response = dict() 
          
        original_object_path = original_object_path.split(" ")[1]        
        url = original_object_path.rsplit("/",2)[0]
        account = url.rsplit("/",1)[1]
        container = original_object_path.rsplit("/",2)[1]
        file_list = source_files_list.split(",")

        swift_conn = swift.Connection(preauthtoken=token, preauthurl=url)

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
        
    def _split(self, source):
        return source.split('?')[0].split('/',2)

    def delete_object(self, source, token):
        response = dict()
        container, obj = self._split(source)
        
        swift.delete_object(url=self.storage_url, 
                            token=token, 
                            container=container, 
                            name=obj, 
                            response_dict=response)
        
        print response      
            
    def set_mc_metadata(self, source, method, mc, metadata, token):
        response = dict() 
        container, obj = self._split(source)
        headers = {'X-Vertigo-on'+method:mc}
        swift.put_object(url = self.storage_url, 
                         token = token,
                         contents = metadata,
                         content_length = len(metadata),
                         container = container, 
                         name = obj, 
                         headers = headers, 
                         response_dict = response)
        
        print response
        
    def get_metadata(self, source, token):
        container, obj = self._split(source)
        metadata = swift.head_object(url = self.storage_url, 
                                     token = token,
                                     container = container, 
                                     name = obj)
        for key in metadata.keys():
            if not key.startswith("x-object-meta"):
                del metadata[key]
        return metadata
        
    def set_metadata(self, source, key, value, token): 
        container, obj = self._split(source)
        response = dict()
        metadata = self.get_metadata(source, token)
        headers = {'x-object-meta-'+key.lower():value}
        metadata.update(headers)

        swift.post_object(url = self.storage_url, 
                          token = token,
                          container = container, 
                          name = obj, 
                          headers = metadata, 
                          response_dict = response)
        
        print response
        
    
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

        data = dtg.get_exec_params()
        
        if command == "SBUS_CMD_EXECUTE":
            self.logger.debug('Do SBUS_CMD_EXECUTE')
            self.logger.debug('Execution information = %s' % str(data))
            
            service = data['service']
            command = data['op']
            token = data['token']
            source = data['source']
            
            if service == "SWIFT":
                
                if command == 'SET_MC_METADATA':
                    method = data['method']
                    mc = data['microcontroller']
                    metadata = data['metadata']
                    self.set_mc_metadata(source, method, mc, metadata, token)
                
                if command == 'SET_METADATA':
                    key = data['key']
                    value = data['value']
                    self.set_metadata(source, key, value, token)
                    
                if command == 'COPY':
                    dest = data['destination']
                    self.copy_object(source, dest, token)
                    
                if command == 'MOVE':
                    dest = data['destination']
                    self.move_object(source, dest, token)
                    
                if command == 'DELETE':
                    self.delete_object(source, token)
               
            """ 
            self._delete_object(prms['swift_token'],prms['source_file'])
            if swift_command == "COPY":
                self._copy_file(prms['swift_token'],prms['source_file'],prms['dest_file'],prms['source_type'])
                
            if swift_command == "PREFETCH":
                self._prefetch_object(prms['swift_token'],prms['object_path'],prms['source_file_list'])
            """
        
    def main_loop(self):
        '''
        The internal loop. Listen to Bus, receive datagram,
        dispatch command, report back.
        '''
        # Create SBus. Listen and process requests
        api_bus = Bus()
        fd = api_bus.create(self.pipe_path)
        if fd < 0:
            self.logger.error("Failed to create Bus. exiting.")
            return

        while True:
            rc = api_bus.listen(fd)

            if rc < 0:
                self.logger.error("Failed to wait on API Bus. exiting.")
                return
            
            self.logger.debug("API Bus wait returned")

            dtg = api_bus.receive(fd)
            if not dtg:
                self.logger.error("Failed to receive message. exiting.")
                return

            self.dispatch_command(dtg)

        self.logger.debug('Leaving main loop')


def start_logger(logger_name, log_level):
    '''
    @param logger_name: The name to report with
    @param log_level:   The verbosity level
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


def usage():
    """
    Prints the correct usage of the API
    """
    print("api_daemon.py <tenant id> <api pipe path> <log level>")


def main(argv):
    if (len(argv) != 3):
        usage()
        return
    
    tenant_id = argv[0]
    pipe_path = argv[1]
    log_level = argv[2]

    logger = start_logger("Microcontroller Framework API", log_level)
    logger.debug("API daemon started")
    Bus.start_logger("DEBUG", container_id="API")
    
    tenant_api = api(tenant_id, pipe_path, logger)
    tenant_api.main_loop()


if __name__ == "__main__":
    main(sys.argv[1:])

