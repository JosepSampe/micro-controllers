'''===========================================================================
19-Oct-2015    josep.sampe    Initial implementation.
==========================================================================='''
from swift.common.internal_client import InternalClient as ic
from swift.common.exceptions import DiskFileNotExist
from swift.common.exceptions import DiskFileXattrNotSupported 
from swift.common.exceptions import DiskFileNoSpace
from swift.obj.diskfile import _get_filename
from swift.common.swob import Request
import xattr
import logging
import pickle
import errno
import os
import time
import subprocess

PICKLE_PROTOCOL = 2
METADATA_KEY = 'user.swift.controller'

INTERNAL_CLIENT = '/etc/swift/storlet-proxy-server.conf'

def read_metadata(fd, md_key = None):
    """
    Helper function to read the pickled metadata from an object file.
    :param fd: file descriptor or filename to load the metadata from
    :returns: dictionary of metadata
    """
    if md_key:
        meta_key = md_key
    else:
        meta_key = METADATA_KEY
        
    metadata = ''
    key = 0
    try:
        while True:
            metadata += xattr.getxattr(fd, '%s%s' % (meta_key,
                                                     (key or '')))
            key += 1
    except (IOError, OSError) as e:
        if metadata =='':
            return False
        for err in 'ENOTSUP', 'EOPNOTSUPP':
            if hasattr(errno, err) and e.errno == getattr(errno, err):
                msg = "Filesystem at %s does not support xattr" % \
                      _get_filename(fd)
                logging.exception(msg)
                raise DiskFileXattrNotSupported(e)
        if e.errno == errno.ENOENT:
            raise DiskFileNotExist()
    return pickle.loads(metadata)


def write_metadata(fd, metadata, xattr_size=65536, md_key = None):
    """
    Helper function to write pickled metadata for an object file.
    :param fd: file descriptor or filename to write the metadata
    :param metadata: metadata to write
    """
    
    if md_key:
        meta_key = md_key
    else:
        meta_key = METADATA_KEY
        
    metastr = pickle.dumps(metadata, PICKLE_PROTOCOL)
    key = 0
    while metastr:
        try:
            xattr.setxattr(fd, '%s%s' % (meta_key, key or ''),
                           metastr[:xattr_size])
            metastr = metastr[xattr_size:]
            key += 1
        except IOError as e:
            for err in 'ENOTSUP', 'EOPNOTSUPP':
                if hasattr(errno, err) and e.errno == getattr(errno, err):
                    msg = "Filesystem at %s does not support xattr" % \
                          _get_filename(fd)
                    logging.exception(msg)
                    raise DiskFileXattrNotSupported(e)
            if e.errno in (errno.ENOSPC, errno.EDQUOT):
                msg = "No space left on device for %s" % _get_filename(fd)
                logging.exception(msg)
                raise DiskFileNoSpace()
            raise

def getAccountMetadata(account):
    iclient = ic(INTERNAL_CLIENT, 'SA', 1)       
    resp = iclient.get_account_metadata(account)
    return resp

def makeSwiftRequest(op, account, container=None, obj=None ):
    iclient = ic(INTERNAL_CLIENT, 'SA', 1)
    path = iclient.make_path(account, container, obj)          
    resp = iclient.make_request(op, path, {'PATH_INFO': path}, [200])
    return resp

def verify_access(self, env, version, account, container, objecte):
    self.logger.info('Verify access to {0}/{1}/{2}'.format(account,
                                                           container,
                                                           objecte))
    new_env = env.copy()
    if 'HTTP_TRANSFER_ENCODING' in new_env.keys():
        del new_env['HTTP_TRANSFER_ENCODING']
    new_env['REQUEST_METHOD'] = 'HEAD'
    new_env['swift.source'] = 'CM'
    new_env['PATH_INFO'] = os.path.join('/' + version, account, container, objecte)
    new_env['RAW_PATH_INFO'] = os.path.join('/' + version, account, container, objecte)
    req = Request.blank(new_env['PATH_INFO'], new_env)

    if 'X-Controller-Onget' in req.headers:
        req.headers.pop('X-Controller-Onget')
        
    if 'X-Controller-Onput' in req.headers:
        req.headers.pop('X-Controller-Onput')

    if 'X-Use-Controller' in req.headers:
        req.headers.pop('X-Use-Controller')
    
    resp = req.get_response(self.app)
    if resp.status_int < 300 and resp.status_int >= 200:
        return True
    return False

def get_file(self, env, version, account, path):

    self.logger.info('Verify access to {0}/{1}'.format(account,path))
    new_env = env.copy()
    if 'HTTP_TRANSFER_ENCODING' in new_env.keys():
        del new_env['HTTP_TRANSFER_ENCODING']
    new_env['REQUEST_METHOD'] = 'GET'
    new_env['swift.source'] = 'CM'
    new_env['PATH_INFO'] = os.path.join('/' + version, account, path)
    new_env['RAW_PATH_INFO'] = os.path.join('/' + version, account, path)
    req = Request.blank(new_env['PATH_INFO'], new_env)

    if 'X-Controller-Onget' in req.headers:
        req.headers.pop('X-Controller-Onget')
        
    if 'X-Controller-Onput' in req.headers:
        req.headers.pop('X-Controller-Onput')

    if 'X-Use-Controller' in req.headers:
        req.headers.pop('X-Use-Controller')
    
    resp = req.get_response(self.app)
    if resp.status_int < 300 and resp.status_int >= 200:
        return resp
    return False

def start_internal_client(self):
    self.logger.info('Swift Controller - Going to execute Internal Client')

    pid = os.popen( "ps -aef | grep -i 'internal_client_daemon.py' | grep -v 'grep' | awk '{ print $2 }'" ).read()

    if pid != "":
        self.logger.info('Swift Controller - Internal Client is already started')
    else:
        #TODO: Change IC path
        cmd='/usr/bin/python /home/lab144/josep/ControllerMiddleware/internal_client_daemon.py' \
            '/home/lxc_device/pipes/scopes/bd34c4073b654/internal_client_pipe DEBUG &'
              
        self.logger.info(cmd)
        p = subprocess.call(cmd,shell=True)
                
        if p == 0:
            self.logger.info('Swift Controller - Internal Client daemon started')   
        else:
            self.logger.info('Swift Controller - Error starting Internal Client daemon')
        
        time.sleep(1)