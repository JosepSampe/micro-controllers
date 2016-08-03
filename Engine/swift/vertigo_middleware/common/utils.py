from swift.common.internal_client import InternalClient
from swift.common.exceptions import DiskFileXattrNotSupported, DiskFileNoSpace, DiskFileNotExist
from swift.common.swob import HTTPUnauthorized, HTTPInternalServerError
from swift.obj.diskfile import get_data_dir as df_data_dir, get_ondisk_files, _get_filename
from swift.common.request_helpers import get_name_and_placement
from swift.common.utils import storage_directory, hash_path
from swift.common.wsgi import make_subrequest
import xattr
import logging
import pickle
import errno
import os


PICKLE_PROTOCOL = 2
VERTIGO_METADATA_KEY = 'user.swift.microcontroller'
SWIFT_METADATA_KEY = 'user.swift.metadata'
LOCAL_PROXY = '/etc/swift/storlet-proxy-server.conf'
DEFAULT_MD_STRING = {'onget': None,
                     'onput': None,
                     'ondelete': None,
                     'ontimer': None}


def read_metadata(fd, md_key=None):
    """
    Helper function to read the pickled metadata from an object file.
    
    :param fd: file descriptor or filename to load the metadata from
    :param md_key: metadata key to be read from object file
    :returns: dictionary of metadata
    """
    if md_key:
        meta_key = md_key
    else:
        meta_key = VERTIGO_METADATA_KEY

    metadata = ''
    key = 0
    try:
        while True:
            metadata += xattr.getxattr(fd, '%s%s' % (meta_key,
                                                     (key or '')))
            key += 1
    except (IOError, OSError) as e:
        if metadata == '':
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


def write_metadata(fd, metadata, xattr_size=65536, md_key=None):
    """
    Helper function to write pickled metadata for an object file.

    :param fd: file descriptor or filename to write the metadata
    :param md_key: metadata key to be write to object file
    :param metadata: metadata to write
    """
    if md_key:
        meta_key = md_key
    else:
        meta_key = VERTIGO_METADATA_KEY

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


def get_swift_metadata(data_file):
    """
    Retrieves the swift metadata of a specified data file
    
    :param data_file: full path of the data file
    :returns: dictionary with all swift metadata
    """
    fd = open_data_file(data_file)
    metadata = read_metadata(fd, SWIFT_METADATA_KEY)
    close_data_file(fd)
    
    return metadata


def set_swift_metadata(data_file, metadata):
    """
    Sets the swift metadata to the specified data file
    
    :param data_file: full path of the data file
    """
    fd = open_data_file(data_file)
    write_metadata(fd, metadata, md_key = SWIFT_METADATA_KEY)
    close_data_file(fd)


def make_swift_request(op, account, container=None, obj=None):
    """
    Makes a swift request via a local proxy (cost expensive)
    
    :param op: opertation (PUT, GET, DELETE, HEAD)
    :param account: swift account
    :param container: swift container
    :param obj: swift object
    :returns: swift.common.swob.Response instance
    """ 
    iclient = InternalClient(LOCAL_PROXY, 'SA', 1)
    path = iclient.make_path(account, container, obj)
    resp = iclient.make_request(op, path, {'PATH_INFO': path}, [200])
    
    return resp


def verify_access(vertigo, path):
    """
    Verifies access to the specified object in swift
    
    :param vertigo: swift_vertigo.vertigo_handler.VertigoProxyHandler instance
    :param path: swift path of the object to check
    :returns: headers of the object whether exists
    """ 
    vertigo.logger.debug('Vertigo - Verify access to %s' % path)
    
    new_env = dict(vertigo.request.environ)
    if 'HTTP_TRANSFER_ENCODING' in new_env.keys():
        del new_env['HTTP_TRANSFER_ENCODING']
    
    for key in DEFAULT_MD_STRING.keys():
        env_key = 'HTTP_X_VERTIGO_' + key.upper()
        if env_key in new_env.keys():
            del new_env[env_key]
    
    auth_token = vertigo.request.headers.get('X-Auth-Token')
    sub_req = make_subrequest(
        new_env, 'HEAD', path,
        headers={'X-Auth-Token': auth_token},
        swift_source='SE')

    resp = sub_req.get_response(vertigo.app)
    
    if not resp.is_success:
        raise HTTPUnauthorized('Failed to verify access to the object',
                               request=vertigo.request)

    return resp.headers


def get_data_dir(vertigo):
    """
    Gets the data directory full path
    
    :param vertigo: swift_vertigo.vertigo_handler.VertigoObjectHandler instance
    :returns: the data directory path
    """ 
    devices = vertigo.conf.get('devices')
    device, partition, account, container, obj, policy = get_name_and_placement(vertigo.request, 5, 5, True)
    name_hash = hash_path(account, container, obj)
    device_path = os.path.join(devices, device)       
    data_dir = os.path.join(device_path, storage_directory(df_data_dir(policy), partition, name_hash))
    
    return data_dir


def get_data_file(vertigo):
    """
    Gets the data file full path
    
    :param vertigo: swift_vertigo.vertigo_handler.VertigoObjectHandler instance
    :returns: the data file path
    """
    data_dir = get_data_dir(vertigo)
    files = os.listdir(data_dir)
    data_file, meta_file, ts_file = get_ondisk_files(files, data_dir)

    return data_file
  
  
def open_data_file(data_file):
    """
    Open a data file
    
    :param data_file: full path of the data file
    :returns: a file descriptor of the open data file
    """ 
    fd = os.open(data_file, os.O_RDONLY)
    return fd


def close_data_file(fd):
    """
    Close a file descriptor
    
    :param fd: file descriptor
    """ 
    os.close(fd) 


def set_microcontroller(vertigo, trigger, mc):
    """
    Sets a microcontroller to the specified object in the main request,
    and stores the metadata file
    
    :param vertigo: swift_vertigo.vertigo_handler.VertigoObjectHandler instance
    :param trigger: storlet name
    :param mc: micro-controller name
    :raises HTTPInternalServerError: If it fails
    """        
    vertigo.logger.debug('Vertigo - Go to assign "' + mc +
                         '" microcontroller to an "' + trigger + '" trigger')
        
    try:
        # Get and Set micro-controller trigger metadata
        microcontroller_dict = get_microcontroller_dict(vertigo)
        #if not microcontroller_dict:
        microcontroller_dict = DEFAULT_MD_STRING
        microcontroller_dict[trigger] = mc
        set_microcontroller_dict(vertigo, microcontroller_dict)
    except:
        vertigo.logger.exception('ERROR getting/setting trigger metadata to object')
        raise HTTPInternalServerError('ERROR getting/setting trigger metadata to object.\n')
    
    try:
        # Write micro-controller metadata file
        data_dir = get_data_dir(vertigo)
        metadata_target_path = os.path.join(data_dir,
                                            mc.rsplit('.', 1)[0] + ".md")
        fn = open(metadata_target_path, 'w')
        fn.write(vertigo.request.body)
        fn.close()
    except:
        vertigo.logger.exception('ERROR writing micro-controller metadata file')
        raise HTTPInternalServerError('ERROR writing micro-controller metadata file.\n')

    vertigo.logger.info('Vertigo - Micro-controller "' + mc + '" assigned correctly')
    vertigo.logger.debug('Vertigo - Object path: ' + data_dir)


def set_microcontroller_dict(vertigo, microcontroller_dict):
    """
    Sets the microcontroller dictionary to the requested object.
    
    :param vertigo: swift_vertigo.vertigo_handler.VertigoObjectHandler instance
    :param microcontroller_list: microcontroller list
    :returns: the microcontroller list
    """
    data_file = get_data_file(vertigo)        
    fd = open_data_file(data_file)
    try:
        return write_metadata(fd, microcontroller_dict)
        close_data_file(fd)
    except Exception as e:
        raise HTTPInternalServerError('ERROR unable to set the dict of microcontrollers: '+str(e))

   
def get_microcontroller_dict(vertigo):
    """
    Gets the list of associated microcontrollers to the requested object.
    This method retrieves a dictionary with all triggers and all microcontrollers
    associated to each trigger.
    
    :param vertigo: swift_vertigo.vertigo_handler.VertigoObjectHandler instance
    :returns: microcontroller dictionary
    """
    data_file = get_data_file(vertigo)
    fd = open_data_file(data_file)    
    try:
        mc_dict = read_metadata(fd)
        close_data_file(fd)
    except Exception as e:
        raise HTTPInternalServerError('ERROR unable to get the microcontroller dict: '+str(e))

    return mc_dict


def get_microcontroller_list(vertigo):
    """
    Gets the list of associated microcontrollers to the requested object.
    This method filters the microcontroller dictionary provided by
    get_microcontroller_list() method, and filter the content to return only 
    a list of names of microcontrollers associated to the type of request (put, 
    get, delete)
    
    :param vertigo: swift_vertigo.vertigo_handler.VertigoObjectHandler instance 
    :returns: microcontroller list associated to the type of the request
    """
    microcontroller_dict = get_microcontroller_dict(vertigo)
    
    mc_list = list()
    if microcontroller_dict:
        mc_list = microcontroller_dict["on" + vertigo.method].split(",")
     
    return mc_list
        