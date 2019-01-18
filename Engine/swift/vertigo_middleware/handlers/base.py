from swift.proxy.controllers.base import get_account_info
from swift.common.swob import HTTPUnauthorized, HTTPBadRequest, Range
from swift.common.utils import config_true_value, cache_from_env
from swift.common.wsgi import make_subrequest
from swift.common.internal_client import InternalClient
from vertigo_middleware.gateways import VertigoGatewayDocker
from vertigo_middleware.gateways import VertigoGatewayStorlet
import os


SYSMETA_OBJ_HEADER = 'X-Object-Transient-Sysmeta-'
MICROCONTROLLERS_OBJ_HEADER = SYSMETA_OBJ_HEADER+'Microcontrollers-'
MICROCONTROLLERS_LIST_OBJ_HEADER = MICROCONTROLLERS_OBJ_HEADER + 'List'

SYSMETA_CONT_HEADER = 'X-Container-Transient-Sysmeta-'
MICROCONTROLLERS_CONT_HEADER = SYSMETA_CONT_HEADER+'Microcontrollers-'
MICROCONTROLLERS_LIST_CONT_HEADER = MICROCONTROLLERS_CONT_HEADER + 'List'

LOCAL_PROXY = '/etc/swift/storlet-proxy-server.conf'
DEFAULT_MD_STRING = {'onget': None,
                     'onput': None,
                     'ondelete': None}


class NotVertigoRequest(Exception):
    pass


def _request_instance_property():
    """
    Set and retrieve the request instance.
    This works to force to tie the consistency between the request path and
    self.vars (i.e. api_version, account, container, obj) even if unexpectedly
    (separately) assigned.
    """

    def getter(self):
        return self._request

    def setter(self, request):
        self._request = request
        try:
            self._extract_vaco()
        except ValueError:
            raise NotVertigoRequest()

    return property(getter, setter,
                    doc="Force to tie the request to acc/con/obj vars")


class VertigoBaseHandler(object):
    """
    This is an abstract handler for Proxy/Object Server middleware
    """
    request = _request_instance_property()

    def __init__(self, request, conf, app, logger):
        """
        :param request: swob.Request instance
        :param conf: gatway conf dict
        """
        self.request = request
        self.vertigo_containers = [conf.get('mc_container'),
                                   conf.get('mc_dependency'),
                                   conf.get('storlet_container'),
                                   conf.get('storlet_dependency')]
        self.available_assignation_headers = ['X-Vertigo-Onget',
                                              'X-Vertigo-Ondelete',
                                              'X-Vertigo-Onput',
                                              'X-Vertigo-Ontimer']
        self.available_deletion_headers = ['X-Vertigo-Onget-Delete',
                                           'X-Vertigo-Ondelete-Delete',
                                           'X-Vertigo-Onput-Delete',
                                           'X-Vertigo-Ontimer-Delete',
                                           'X-Vertigo-Delete']

        self.app = app
        self.logger = logger
        self.conf = conf
        self.method = self.request.method.lower()
        self.execution_server = conf["execution_server"]

    def _setup_docker_gateway(self, response=None):
        self.request.headers['X-Current-Server'] = self.execution_server
        self.request.headers['X-Method'] = self.method
        self.mc_docker_gateway = VertigoGatewayDocker(
                                    self.request, response,
                                    self.conf, self.logger, self.account)

    def _setup_storlet_gateway(self):
        self.storlet_gateway = VertigoGatewayStorlet(
            self.conf, self.logger, self.app, self.api_version,
            self.account, self.request.method)

    def _extract_vaco(self):
        """
        Set version, account, container, obj vars from self._parse_vaco result
        :raises ValueError: if self._parse_vaco raises ValueError while
                            parsing, this method doesn't care and raise it to
                            upper caller.
        """
        self._api_version, self._account, self._container, self._obj = \
            self._parse_vaco()

    def get_mc_assignation_data(self):
        header = [i for i in self.available_assignation_headers
                  if i in self.request.headers.keys()]
        if len(header) > 1:
            raise HTTPUnauthorized('Vertigo - The system can only set 1'
                                   ' microcontroller each time.\n')
        mc = self.request.headers[header[0]]

        return header[0].rsplit('-', 1)[1].lower(), mc

    def get_mc_deletion_data(self):
        header = [i for i in self.available_deletion_headers
                  if i in self.request.headers.keys()]
        if len(header) > 1:
            raise HTTPUnauthorized('Vertigo - The system can only delete 1'
                                   ' microcontroller each time.\n')
        mc = self.request.headers[header[0]]

        return header[0].rsplit('-', 2)[1].lower(), mc

    @property
    def api_version(self):
        return self._api_version

    @property
    def account(self):
        return self._account

    @property
    def container(self):
        return self._container

    @property
    def obj(self):
        return self._obj

    def _parse_vaco(self):
        """
        Parse method of path from self.request which depends on child class
        (Proxy or Object)

        :return tuple: a string tuple of (version, account, container, object)
        """
        raise NotImplementedError()

    def handle_request(self):
        """
        Run Vertigo
        """
        raise NotImplementedError()

    @property
    def is_storlet_execution(self):
        """
        Check if the request requires storlet execution

        :return: Whether storlet should be executed
        """
        return 'X-Run-Storlet' in self.request.headers

    @property
    def is_range_request(self):
        """
        Determines whether the request is a byte-range request
        """
        return 'Range' in self.request.headers

    @property
    def is_storlet_range_request(self):
        return 'X-Storlet-Range' in self.request.headers

    @property
    def is_storlet_multiple_range_request(self):
        if not self.is_storlet_range_request:
            return False

        r = self.request.headers['X-Storlet-Range']
        return len(Range(r).ranges) > 1

    @property
    def is_vertigo_container_request(self):
        """
        Determines whether the request is over any vertigo container
        """
        return self.container in self.vertigo_containers

    @property
    def is_vertigo_object_put(self):
        return (self.container in self.vertigo_containers and self.obj and
                self.request.method == 'PUT')

    @property
    def is_slo_get_request(self):
        """
        Determines from a GET request and its  associated response
        if the object is a SLO
        """
        return self.request.params.get('multipart-manifest') == 'get'

    @property
    def is_copy_request(self):
        """
        Determines from a GET request if is a copy request
        """
        return 'X-Copy-From' in self.request.headers

    @property
    def is_mc_disabled(self):
        if 'mc-enabled' in self.request.headers:
            return self.request.headers['mc-enabled'] == 'False'
        else:
            return False

    @property
    def is_valid_request(self):
        """
        Determines if is a Vertigo valid request
        """
        return not any([self.is_copy_request, self.is_slo_get_request,
                        self.is_mc_disabled, self.is_vertigo_container_request,
                        not ((not self.obj and self.request.method == 'HEAD') or
                             (self.obj))])

    @property
    def is_trigger_assignation(self):
        return any((True for x in self.available_assignation_headers
                    if x in self.request.headers.keys()))

    @property
    def is_trigger_deletion(self):
        return any((True for x in self.available_deletion_headers
                    if x in self.request.headers.keys()))

    @property
    def is_object_move(self):
        return 'X-Vertigo-Link-To' in self.request.headers

    def is_slo_response(self, resp):
        self.logger.debug(
            'Verify if {0}/{1}/{2} is an SLO assembly object'.format(
                self.account, self.container, self.obj))
        is_slo = 'X-Static-Large-Object' in resp.headers
        if is_slo:
            self.logger.debug(
                '{0}/{1}/{2} is indeed an SLO assembly '
                'object'.format(self.account, self.container, self.obj))
        else:
            self.logger.debug(
                '{0}/{1}/{2} is NOT an SLO assembly object'.format(
                    self.account, self.container, self.obj))
        return is_slo

    def is_account_storlet_enabled(self):
        account_meta = get_account_info(self.request.environ, self.app)['meta']
        storlets_enabled = account_meta.get('storlet-enabled', 'False')
        if not config_true_value(storlets_enabled):
            self.logger.debug('Vertigo - Account disabled for storlets')
            raise HTTPBadRequest('Vertigo - Error: Account disabled for'
                                 ' storlets.\n', request=self.request)
        return True

    def apply_storlet_on_get(self, resp, storlet_list):
        """
        Call gateway module to get result of storlet execution
        in GET flow
        """
        self._setup_storlet_gateway()
        data_iter = resp.app_iter
        response = self.storlet_gateway.run(resp, storlet_list, data_iter)

        if 'Content-Length' in response.headers:
            response.headers.pop('Content-Length')
        if 'Transfer-Encoding' in response.headers:
            response.headers.pop('Transfer-Encoding')
        if 'Etag' in response.headers:
            response.headers['Etag'] = ''

        return response

    def apply_storlet_on_put(self, req, storlet_list):
        """
        Call gateway module to get result of storlet execution
        in PUT flow
        """
        self._setup_storlet_gateway()
        data_iter = req.environ['wsgi.input']
        self.request = self.storlet_gateway.run(req, storlet_list, data_iter)

        if 'CONTENT_LENGTH' in self.request.environ:
            self.request.environ.pop('CONTENT_LENGTH')
        self.request.headers['Transfer-Encoding'] = 'chunked'

    def get_object_metadata(self, obj):
        """
        Retrieves the swift metadata of a specified object

        :param obj: object name
        :returns: dictionary with all swift metadata
        """
        new_env = dict(self.request.environ)
        auth_token = self.request.headers.get('X-Auth-Token')
        dest_path = os.path.join('/', self.api_version, self.account, self.container, obj)
        sub_req = make_subrequest(new_env, 'HEAD', dest_path,
                                  headers={'X-Auth-Token': auth_token},
                                  swift_source='micro-controllers_middleware')
        response = sub_req.get_response(self.app)

        return response.headers

    def get_container_metadata(self, container):
        """
        Retrieves the swift metadata of the request container

        :returns: dictionary with all swift metadata
        """
        new_env = dict(self.request.environ)
        auth_token = self.request.headers.get('X-Auth-Token')
        dest_path = os.path.join('/', self.api_version, self.account, container)
        sub_req = make_subrequest(new_env, 'HEAD', dest_path,
                                  headers={'X-Auth-Token': auth_token},
                                  swift_source='micro-controllers_middleware')
        response = sub_req.get_response(self.app)

        return response.headers

    def set_object_metadata(self, obj, metadata):
        """
        Sets the swift metadata to the specified data_file

        :param metadata: metadata dictionary
        :param obj: object name
        """
        for key in metadata.keys():
            if not key.startswith(MICROCONTROLLERS_OBJ_HEADER):
                del metadata[key]

        new_env = dict(self.request.environ)
        auth_token = self.request.headers.get('X-Auth-Token')
        metadata.update({'X-Auth-Token': auth_token})
        dest_path = os.path.join('/', self.api_version, self.account, self.container, obj)
        sub_req = make_subrequest(new_env, 'POST', dest_path,
                                  headers=metadata,
                                  swift_source='micro-controllers_middleware')
        response = sub_req.get_response(self.app)

        print(response.body)

    def set_container_metadata(self, container, metadata):
        """
        Sets the swift metadata to the container

        :param metadata: metadata dictionary
        """
        for key in metadata.keys():
            if not key.startswith(MICROCONTROLLERS_CONT_HEADER):
                del metadata[key]
        dest_path = os.path.join('/', self.api_version, self.account, container)
        # We store the micro-controller execution list in a memcached server (only 10 minutes)
        memcache = cache_from_env(self.request.environ)
        memcache.set("vertigo_"+dest_path, metadata, time=600)
        new_env = dict(self.request.environ)
        auth_token = self.request.headers.get('X-Auth-Token')
        metadata.update({'X-Auth-Token': auth_token})
        sub_req = make_subrequest(new_env, 'POST', dest_path,
                                  headers=metadata,
                                  swift_source='micro-controllers_middleware')
        sub_req.get_response(self.app)

    def verify_access(self, path):
        """
        Verifies access to the specified object in swift

        :param vertigo: swift_vertigo.vertigo_handler.VertigoProxyHandler instance
        :param path: swift path of the object to check
        :returns: headers of the object whether exists
        """
        self.logger.debug('Vertigo - Verify access to %s' % path)

        new_env = dict(self.request.environ)
        if 'HTTP_TRANSFER_ENCODING' in new_env.keys():
            del new_env['HTTP_TRANSFER_ENCODING']

        for key in DEFAULT_MD_STRING.keys():
            env_key = 'HTTP_X_VERTIGO_' + key.upper()
            if env_key in new_env.keys():
                del new_env[env_key]

        auth_token = self.request.headers.get('X-Auth-Token')
        sub_req = make_subrequest(new_env, 'HEAD', path,
                                  headers={'X-Auth-Token': auth_token},
                                  swift_source='micro-controllers_middleware')

        return sub_req.get_response(self.app)

    def set_microcontroller_container(self, trigger, mc):
        """
        Sets a microcontroller to the specified container in the main request,
        and stores the metadata file

        :param vertigo: swift_vertigo.vertigo_handler.VertigoObjectHandler instance
        :param trigger: trigger name
        :param mc: microcontroller name
        :raises HTTPInternalServerError: If it fails
        """
        # 1st: set micro-controller name to list
        try:
            metadata = self.get_container_metadata(self.container)
            mc_dict = self.get_microcontroller_dict_container(metadata)
        except:
            raise ValueError('ERROR: There was an error getting trigger'
                             ' dictionary from the object.\n')

        if not mc_dict:
            mc_dict = DEFAULT_MD_STRING
        if not mc_dict[trigger]:
            mc_dict[trigger] = list()
        if mc not in mc_dict[trigger]:
            mc_dict[trigger].append(mc)

        # 2nd: Get micro-controller parameters
        mc_parameters = self.request.body.rstrip()

        # 3rd: Assign all metadata to the container
        try:
            metadata[MICROCONTROLLERS_LIST_CONT_HEADER] = mc_dict
            sysmeta_key = (MICROCONTROLLERS_CONT_HEADER + trigger + '-' + mc).title()
            if mc_parameters:
                metadata[sysmeta_key] = mc_parameters
            else:
                if sysmeta_key in metadata:
                    del metadata[sysmeta_key]
            self.set_container_metadata(self.container, metadata)
        except:
            raise ValueError('ERROR: There was an error setting trigger'
                             ' dictionary from the object.\n')

    def delete_microcontroller_container(self, trigger, mc):
        """
        Deletes a micro-controller to the specified object in the main request

        :param vertigo: swift_vertigo.vertigo_handler.VertigoObjectHandler instance
        :param trigger: trigger name
        :param mc: micro-controller name
        :raises HTTPInternalServerError: If it fails
        """
        self.logger.debug('Going to delete "' + mc + '" micro-controller from "' +
                          trigger + '" trigger')

        metadata = self.get_container_metadata(self.container)
        try:
            mc_dict = self.get_microcontroller_dict_container(metadata)
        except:
            raise ValueError('ERROR: There was an error getting trigger'
                             ' metadata from the object.\n')

        try:
            if trigger == "vertigo" and mc == "all":
                for key in metadata.keys():
                    if key.startswith(MICROCONTROLLERS_CONT_HEADER):
                        del metadata[key]
            else:
                if metadata[MICROCONTROLLERS_LIST_CONT_HEADER]:
                    if isinstance(metadata[MICROCONTROLLERS_LIST_CONT_HEADER], dict):
                        mc_dict = metadata[MICROCONTROLLERS_LIST_CONT_HEADER]
                    else:
                        mc_dict = eval(metadata[MICROCONTROLLERS_LIST_CONT_HEADER])

                    if mc == 'all':
                        mc_list = mc_dict[trigger]
                        mc_dict[trigger] = None
                        for mc_k in mc_list:
                            sysmeta_key = (MICROCONTROLLERS_CONT_HEADER + trigger + '-' + mc_k).title()
                            if sysmeta_key in metadata:
                                metadata[sysmeta_key] = ''
                    elif mc in mc_dict[trigger]:
                        mc_dict[trigger].remove(mc)
                        sysmeta_key = (MICROCONTROLLERS_CONT_HEADER + trigger + '-' + mc).title()
                        if sysmeta_key in metadata:
                            metadata[sysmeta_key] = ''
                    else:
                        raise

                    metadata[MICROCONTROLLERS_LIST_CONT_HEADER] = mc_dict
                    metadata = self.clean_microcontroller_dict_container(metadata)
                else:
                    raise

            self.set_container_metadata(self.container, metadata)
        except:
            pass
            # raise ValueError('Vertigo - Error: Micro-controller "' + mc + '" not'
            #                 ' assigned to the "' + trigger + '" trigger.\n')

    def set_microcontroller_object(self, trigger, mc, obj):
        """
        Sets a micro-controller to the specified object in the main request,
        and stores the parameters

        :param vertigo: swift_vertigo.vertigo_handler.VertigoObjectHandler instance
        :param trigger: trigger name
        :param mc: micro-controller name
        :raises HTTPInternalServerError: If it fails
        """

        # 1st: set micro-controller dictionary
        try:
            metadata = self.get_object_metadata(obj)
            mc_dict = self.get_microcontroller_dict_object(obj)
        except:
            raise ValueError('ERROR: There was an error getting trigger'
                             ' dictionary from the object.\n')

        if not mc_dict:
            mc_dict = DEFAULT_MD_STRING
        if not mc_dict[trigger]:
            mc_dict[trigger] = list()
        if mc not in mc_dict[trigger]:
            mc_dict[trigger].append(mc)

        # 2nd: Get  micro-controller parameters
        mc_parameters = self.request.body.rstrip()

        # 3rd: Assign all metadata to the object
        try:
            metadata[MICROCONTROLLERS_LIST_OBJ_HEADER] = mc_dict
            sysmeta_key = (MICROCONTROLLERS_OBJ_HEADER + trigger + '-' + mc).title()
            if mc_parameters:
                metadata[sysmeta_key] = mc_parameters
            else:
                if sysmeta_key in metadata:
                    del metadata[sysmeta_key]
            self.set_object_metadata(obj, metadata)
        except:
            raise ValueError('ERROR: There was an error setting trigger'
                             ' dictionary to the object.\n')

    def delete_microcontroller_object(self, trigger, mc, obj):
        """
        Deletes a micro-controller to the specified object in the main request

        :param vertigo: swift_vertigo.vertigo_handler.VertigoObjectHandler instance
        :param trigger: trigger name
        :param mc: microcontroller name
        :raises HTTPInternalServerError: If it fails
        """
        self.logger.debug('Going to delete "' + mc +
                          '" micro-controller from "' + trigger + '" trigger')

        try:
            metadata = self.get_object_metadata(obj)
        except:
            raise ValueError('ERROR: There was an error getting trigger'
                             ' metadata from the object.\n')

        try:
            if trigger == "vertigo" and mc == "all":
                for key in metadata.keys():
                    if key.startswith(MICROCONTROLLERS_OBJ_HEADER):
                        del metadata[key]
            else:
                if metadata[MICROCONTROLLERS_LIST_OBJ_HEADER]:
                    if isinstance(metadata[MICROCONTROLLERS_LIST_OBJ_HEADER], dict):
                        mc_dict = metadata[MICROCONTROLLERS_LIST_OBJ_HEADER]
                    else:
                        mc_dict = eval(metadata[MICROCONTROLLERS_LIST_OBJ_HEADER])
                    if mc == 'all':
                        mc_list = mc_dict[trigger]
                        mc_dict[trigger] = None
                        for mc_k in mc_list:
                            sysmeta_key = (MICROCONTROLLERS_OBJ_HEADER + trigger + '-' + mc_k).title()
                            if sysmeta_key in metadata:
                                del metadata[sysmeta_key]
                    elif mc in mc_dict[trigger]:
                        mc_dict[trigger].remove(mc)
                        sysmeta_key = (MICROCONTROLLERS_OBJ_HEADER + trigger + '-' + mc).title()
                        if sysmeta_key in metadata:
                            del metadata[sysmeta_key]
                    else:
                        raise
                    metadata[MICROCONTROLLERS_LIST_OBJ_HEADER] = mc_dict
                    metadata = self.clean_microcontroller_dict_object(metadata)
                else:
                    raise
            self.set_object_metadata(obj, metadata)
        except:
            raise ValueError('Error: Micro-controller "' + mc + '" not'
                             ' assigned to the "' + trigger + '" trigger.\n')

    def clean_microcontroller_dict_object(self, metadata):
        """
        Auxiliary function that cleans the micro-controller dictionary, deleting
        empty lists for each trigger, and deleting all dictionary whether all
        values are None.

        :param microcontroller_dict: micro-controller dictionary
        :returns microcontroller_dict: micro-controller dictionary
        """
        for trigger in metadata[MICROCONTROLLERS_LIST_OBJ_HEADER].keys():
            if not metadata[MICROCONTROLLERS_LIST_OBJ_HEADER][trigger]:
                metadata[MICROCONTROLLERS_LIST_OBJ_HEADER][trigger] = None

        if all(value is None for value in metadata[MICROCONTROLLERS_LIST_OBJ_HEADER].values()):
            del metadata[MICROCONTROLLERS_LIST_OBJ_HEADER]

        return metadata

    def clean_microcontroller_dict_container(self, metadata):
        """
        Auxiliary function that cleans the micro-controller dictionary, deleting
        empty lists for each trigger, and deleting all dictionary whether all
        values are None.

        :param metadata: micro-controller dictionary
        :returns metadata: micro-controller dictionary
        """
        mc_dict = eval(metadata[MICROCONTROLLERS_LIST_CONT_HEADER])
        for trigger in mc_dict.keys():
            if not mc_dict[trigger]:
                mc_dict[trigger] = None

        if all(value is None for value in mc_dict.values()):
            metadata[MICROCONTROLLERS_LIST_CONT_HEADER] = ''

        return metadata

    def get_microcontroller_dict_object(self, metadata):
        """
        Gets the list of associated micro-controllers to the requested object.
        This method retrieves a dictionary with all triggers and all
        micro-controllers associated to each trigger.

        :param vertigo: swift_vertigo.vertigo_handler.VertigoObjectHandler instance
        :returns: microcontroller dictionary
        """
        if MICROCONTROLLERS_LIST_OBJ_HEADER in metadata:
            if isinstance(metadata[MICROCONTROLLERS_LIST_OBJ_HEADER], dict):
                return metadata[MICROCONTROLLERS_LIST_OBJ_HEADER]
            else:
                return eval(metadata[MICROCONTROLLERS_LIST_OBJ_HEADER])
        else:
            return None

    def get_microcontroller_dict_container(self, metadata):
        """
        Gets the list of associated micro-controllers to the requested container.
        This method retrieves a dictionary with all triggers and all
        micro-controllers associated to each trigger.

        :param vertigo: swift_vertigo.vertigo_handler.VertigoProxyHandler instance
        :returns: microcontroller dictionary
        """
        if MICROCONTROLLERS_LIST_CONT_HEADER in metadata:
            if isinstance(metadata[MICROCONTROLLERS_LIST_CONT_HEADER], dict):
                return metadata[MICROCONTROLLERS_LIST_CONT_HEADER]
            else:
                return eval(metadata[MICROCONTROLLERS_LIST_CONT_HEADER])
        else:
            return None

    def get_microcontroller_list_object(self, headers, method):
        """
        Gets the list of associated micro-controllers to the requested object.
        This method gets the micro-controller dictionary from the object headers,
        and filter the content to return only a list of names of micro-controllers
        associated to the type of request (put, get, delete)

        :param headers: response headers of the object
        :param method: current method
        :returns: microcontroller list associated to the type of the request
        """
        if MICROCONTROLLERS_LIST_OBJ_HEADER in headers:
            if isinstance(headers[MICROCONTROLLERS_LIST_OBJ_HEADER], dict):
                microcontroller_dict = headers[MICROCONTROLLERS_LIST_OBJ_HEADER]
            else:
                microcontroller_dict = eval(headers[MICROCONTROLLERS_LIST_OBJ_HEADER])
            mc_list = microcontroller_dict["on" + method]
        else:
            mc_list = None

        return mc_list
