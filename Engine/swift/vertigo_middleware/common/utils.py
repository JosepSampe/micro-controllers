from swift.common.internal_client import InternalClient
from swift.common.exceptions import DiskFileXattrNotSupported, DiskFileNoSpace
from swift.common.exceptions import DiskFileNotExist
from swift.obj.diskfile import _get_filename
import xattr
import logging
import pickle
import errno
import os

PICKLE_PROTOCOL = 2
SWIFT_METADATA_KEY = 'user.swift.metadata'
LOCAL_PROXY = '/etc/swift/storlet-proxy-server.conf'


def read_metadata(fd, md_key=None):
    """
    Helper function to read the pickled metadata from an object file.
    :param fd: file descriptor or filename to load the metadata from
    :param md_key: metadata key to be read from object file
    :returns: dictionary of metadata
    """
    meta_key = SWIFT_METADATA_KEY

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
    meta_key = SWIFT_METADATA_KEY

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


def get_object_metadata(data_file):
    """
    Retrieves the swift metadata of a specified data file
    :param data_file: full path of the data file
    :returns: dictionary with all swift metadata
    """
    fd = open_data_file(data_file)
    metadata = read_metadata(fd, SWIFT_METADATA_KEY)
    close_data_file(fd)

    return metadata


def set_object_metadata(data_file, metadata):
    """
    Sets the swift metadata to the specified data_file
    :param data_file: full path of the data file
    """
    fd = open_data_file(data_file)
    write_metadata(fd, metadata, md_key=SWIFT_METADATA_KEY)
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
