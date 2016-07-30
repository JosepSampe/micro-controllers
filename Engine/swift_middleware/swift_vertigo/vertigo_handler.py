from swift.proxy.controllers.base import get_account_info
from swift.common.swob import HTTPInternalServerError, HTTPMethodNotAllowed, HTTPUnauthorized, HTTPException, HTTPBadRequest, wsgify, Response, Range
from swift.common.utils import config_true_value, public, cache_from_env, get_logger
from vertigo_storlet_gateway import VertigoGatewayStorlet
from vertigo_docker_gateway import VertigoGatewayDocker
from vertigo_common import verify_access, get_microcontroller_list, get_microcontroller_dict, set_microcontroller
import ConfigParser
import pickle
import json
import time


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


class BaseVertigoHandler(object):
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
        self.available_triggers = ['X-Vertigo-Onget',
                                   'X-Vertigo-Ondelete',
                                   'X-Vertigo-Onput',
                                   'X-Vertigo-Ontimer']
        self.app = app
        self.logger = logger
        self.conf = conf
        self.method = self.request.method.lower()

    def _setup_docker_gateway(self, original_resp=None):
        self.mc_docker_gateway = VertigoGatewayDocker(
            self.request, original_resp,
            self.conf, self.logger, self.app,
            self.account)

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

    def get_vertigo_mc_data(self):
        header = [i for i in self.available_triggers
                  if i in self.request.headers.keys()]
        if len(header) > 1:
            raise HTTPUnauthorized('The system can only set 1 controller' +
                                   ' each time.\n')
        mc = self.request.headers[header[0]]

        return header[0].rsplit('-', 1)[1].lower(), mc

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
        pass

    @property
    def is_trigger_assignation(self):
        return any((True for x in self.available_triggers
                    if x in self.request.headers.keys()))


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
        account_meta = get_account_info(self.request.environ,
                                        self.app)['meta']
        storlets_enabled = account_meta.get('storlet-enabled',
                                            'False')
        if not config_true_value(storlets_enabled):
            self.logger.debug('Vertigo - Account disabled for storlets')
            return HTTPBadRequest('Vertigo - Account disabled for storlets',
                                  request=self.request)

        return True

    def _call_storlet_gateway(self, resp):
        """
        Call gateway module to get result of storlet execution
        in GET flow
        """
        raise NotImplementedError()

    def apply_storlet_on_get(self, resp, storlet_list):
        resp = self._call_storlet_gateway_on_get(resp, storlet_list)

        if 'Content-Length' in resp.headers:
            resp.headers.pop('Content-Length')
        if 'Transfer-Encoding' in resp.headers:
            resp.headers.pop('Transfer-Encoding')

        return resp

    def apply_storlet_on_put(self, req, storlet_list):
        self.request = self._call_storlet_gateway_on_put(req, storlet_list)

        if 'CONTENT_LENGTH' in self.request.environ:
            self.request.environ.pop('CONTENT_LENGTH')
        self.request.headers['Transfer-Encoding'] = 'chunked'


class VertigoProxyHandler(BaseVertigoHandler):

    def __init__(self, request, conf, app, logger):
        super(VertigoProxyHandler, self).__init__(
            request, conf, app, logger)

        self.mc_container = self.conf["mc_container"]
        self.memcache = None

    def _parse_vaco(self):
        return self.request.split_path(4, 4, rest_with_last=True)

    def is_object_in_cache(self):

        if self.memcache is None:
            self.memcache = cache_from_env(self.request.environ)

        self.cached_object = self.memcache.get(self.account + "/" +
                                               self.container + "/" + self.obj)
        self.logger.info('Vertigo - Checking in cache: ' + self.account + "/" +
                         self.container + "/" + self.obj)

        return self.cached_object is not None

    @property
    def is_vertigo_object_put(self):
        return (self.container in self.vertigo_containers and self.obj
                and self.request.method == 'PUT')
        
    def handle_request(self):
        if hasattr(self, self.request.method):
            try:
                handler = getattr(self, self.request.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                return HTTPMethodNotAllowed(request=self.request)
            return handler()
        else:
            return self.request.get_response(self.app)
            # un-defined method should be NOT ALLOWED
            # return HTTPMethodNotAllowed(request=self.request)
        
    def _get_microcontrollers_for_this_request(self, object_metadata):
        mc_list = dict()
        if object_metadata['Vertigo-Micro-Controller-List']:
            microcontroller_list = eval(object_metadata['Vertigo-Micro-Controller-List'])
            if microcontroller_list:
                mc_list = microcontroller_list["on" + self.method].split(",")
        return mc_list
    
    def _get_microcontroller_metadata(self, mc_list):
        mc_metadata = dict()
        for mc in mc_list:
            objpath = '/'.join(['', self.api_version, self.account,
                               self.mc_container, mc])
            mc_metadata[mc] = verify_access(self, objpath)
        
        return mc_metadata
   
    def _augment_vertigo_request(self, mc_metadata):
        if mc_metadata:
            self.request.headers['Micro-controller-Metadata'] = mc_metadata
        
    def _call_storlet_gateway_on_put(self, req, storlet_list):
        req, app_iter = self.storlet_gateway.execute_storlet(req, storlet_list)
        req.environ['wsgi.input'] = app_iter
        return req

    def _call_storlet_gateway_on_get(self, resp, storlet_list):
        resp, app_iter = self.storlet_gateway.execute_storlet(
            resp, storlet_list)
        resp.app_iter = app_iter
        resp.headers.pop('Vertigo')
        resp.headers.pop("Storlet-Executed")
        return resp

    @public
    def GET(self):
        """
        GET handler on Proxy
        """
        if self.is_object_in_cache():
            value = pickle.loads(self.cached_object)

            self.logger.info('Vertigo - OBJECT IN CACHE')

            resp_headers = value["Headers"]
            resp_headers['content-length'] = len(value["Body"])

            return Response(body=value["Body"],
                            headers=resp_headers,
                            request=self.request)
        else:
            # objpath = '/'.join(['', self.api_version, self.account,
            #                   self.container, self.obj])
            # object_metadata = verify_access(self, objpath)
            # mc_list = self._get_microcontrollers_for_this_request(object_metadata)
            #  mc_metadata = self._get_microcontroller_metadata(mc_list)
            # self._augment_vertigo_request(mc_metadata)
   
            original_resp = self.request.get_response(self.app)

        if 'Vertigo' in original_resp.headers and \
                self.is_account_storlet_enabled():
            # There are storlets to execute on proxy side
            self.logger.info('Vertigo - There are Storlets to execute')

            self._setup_storlet_gateway()
            storlet_list = json.loads(original_resp.headers['Vertigo'])
            return self.apply_storlet_on_get(original_resp, storlet_list)

        # There are no storlets to execute on proxy side
        elif 'Storlet-Executed' in original_resp.headers:
            #  Storlet was already invoked at object side
            if 'Transfer-Encoding' in original_resp.headers:
                original_resp.headers.pop('Transfer-Encoding')
            original_resp.headers['Content-Length'] = None

        return original_resp

    @public
    def PUT(self):
        """
        PUT handler on Proxy
        """
        if self.is_trigger_assignation:
            # Only enters here when a user assign a micro-controller to an object
            _, micro_controller = self.get_vertigo_mc_data()
            # Verify if the object is in Swift
            objpath = '/'.join(['', self.api_version, self.account,
                              self.container, self.obj])
            if not verify_access(self, objpath):
                return HTTPUnauthorized('Vertigo - Object error: Perhaps '
                                        +self.obj+' doesn\'t exists in Swift.\n')

            # Verify if the micro-controller is in Swift
            mcpath = '/'.join(['', self.api_version, self.account,
                              self.mc_container, micro_controller])
            if not verify_access(self, mcpath):
                return HTTPUnauthorized('Vertigo -  MicroController error: '+
                                        'Perhaps ' + micro_controller + 
                                        ' doesn\'t exists in Swift.\n')

        return self.request.get_response(self.app)


class VertigoObjectHandler(BaseVertigoHandler):

    def __init__(self, request, conf, app, logger):
        super(VertigoObjectHandler, self).__init__(
            request, conf, app, logger)

    def _parse_vaco(self):
        _, _, acc, cont, obj = self.request.split_path(
            5, 5, rest_with_last=True)
        return ('v1', acc, cont, obj)

    @property
    def is_slo_get_request(self):
        """
        Determines from a GET request and its  associated response
        if the object is a SLO
        """
        return self.request.params.get('multipart-manifest') == 'get'

    def handle_request(self):
        if hasattr(self, self.request.method):
            try:
                handler = getattr(self, self.request.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                return HTTPMethodNotAllowed(request=self.request)
            return handler()
        else:
            return self.request.get_response(self.app)
            # un-defined method should be NOT ALLOWED
            # return HTTPMethodNotAllowed(request=self.request)

    def _call_storlet_gateway_on_put(self, req, storlet_list):
        req, app_iter = self.storlet_gateway.execute_storlet(req, storlet_list)
        req.environ['wsgi.input'] = app_iter
        req.headers.pop('Storlet-Executed')
        return req

    def _call_storlet_gateway_on_get(self, resp, storlet_list):
        resp, app_iter = self.storlet_gateway.execute_storlet(
            resp, storlet_list)
        resp.app_iter = app_iter
        return resp

    @public
    def GET(self):
        """
        GET handler on Object
        """
        start = time.clock()
 
        response = self.request.get_response(self.app)
        mc_list = get_microcontroller_list(self)
        
        if mc_list:
            self.logger.info('Vertigo - There are micro-controllers' +
                             ' to execute')
            
            self._setup_docker_gateway(response)
            # Go to run the micro-controller(s)
            storlet_list = self.mc_docker_gateway.execute_microcontrollers(mc_list)

            self.logger.info('Vertigo - Micro-Controller executed correctly')
            # Go to run the Storlet(s) whether the microcontroller
            # sends back any.

            if storlet_list:
                self.logger.info('Vertigo - Go to execute Storlets: '+str(storlet_list))
                self._setup_storlet_gateway()
                return self.apply_storlet_on_get(response, storlet_list)
            else:
                self.logger.info('Vertigo - No Storlets to execute')
        else:
            self.logger.info('Vertigo - No Micro-Controllers to execute')
            
        end = time.clock() - start
        print end
        
        # f = open('/home/lab144/josep/middleware_get_tstamp.log','a')
        # f.write(str(end)+'\n') # python will convert \n to os.linesep
        # f.close()

        return response

    @public
    def PUT(self):
        """
        PUT handler on Object
        """
        if self.is_trigger_assignation:
            # Only enters here when a user assign a micro-controller to an object
            trigger, micro_controller = self.get_vertigo_mc_data()
            set_microcontroller(self, trigger, micro_controller)
            
            return Response(body='Vertigo micro-controller "'+micro_controller+
                                 '" assigned correctly to an "'+ trigger +
                                 '" trigger.\n',
                            headers={'etag':''},
                            request=self.request)
        else:
            return self.request.get_response(self.app)
      
    @public
    def HEAD(self):
        """
        HEAD handler on Object
        """
        original_resp = self.request.get_response(self.app)
        mc_list = get_microcontroller_dict(self)
        for trigger in mc_list.keys():
            if not mc_list[trigger]:
                del mc_list[trigger]
        original_resp.headers['Vertigo-Micro-Controller-List'] = mc_list

        return original_resp
    

class VertigoHandlerMiddleware(object):

    def __init__(self, app, conf, vertigo_conf):
        self.app = app
        self.exec_server = vertigo_conf.get('execution_server')
        self.logger = get_logger(conf, log_route='vertigo_handler')
        self.vertigo_conf = vertigo_conf
        self.vertigo_containers = [vertigo_conf.get('mc_container'),
                                   vertigo_conf.get('mc_dependency'),
                                   vertigo_conf.get('storlet_container'),
                                   vertigo_conf.get('storlet_dependency')]
        self.available_triggers = ['X-Vertigo-Onget',
                                   'X-Vertigo-Ondelete',
                                   'X-Vertigo-Onput',
                                   'X-Vertigo-Ontimer']
        self.handler_class = self._get_handler(self.exec_server)

    def _get_handler(self, exec_server):
        """
        Generate Handler class based on execution_server parameter
        
        :param exec_server: Where this storlet_middleware is running.
                            This should value shoud be 'proxy' or 'object'
        :raise ValueError: If exec_server is invalid
        """
        if exec_server == 'proxy':
            return VertigoProxyHandler
        elif exec_server == 'object':
            return VertigoObjectHandler
        else:
            raise ValueError(
                'configuration error: execution_server must be either proxy'
                ' or object but is %s' % exec_server)

    @wsgify
    def __call__(self, req):
        try:
            request_handler = self.handler_class(
                req, self.vertigo_conf, self.app, self.logger)
            self.logger.debug('vertigo_handler call in %s with %s/%s/%s' %
                              (self.exec_server, request_handler.account,
                               request_handler.container,
                               request_handler.obj))
        except HTTPException:
            raise
        except NotVertigoRequest:
            return req.get_response(self.app)

        try:
            return request_handler.handle_request()
        
        except HTTPException:
            self.logger.exception('Vertigo execution failed')
            raise
        except Exception:
            self.logger.exception('Vertigo execution failed')
            raise HTTPInternalServerError(body='Vertigo execution failed')


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""
    
    conf = global_conf.copy()
    conf.update(local_conf)

    vertigo_conf = dict()
    vertigo_conf['devices'] = conf.get('devices', '/srv/node')
    vertigo_conf['execution_server'] = conf.get('execution_server')
    vertigo_conf['mc_timeout'] = conf.get('mc_timeout', 20)
    vertigo_conf['mc_pipe'] = conf.get('mc_pipe', 'vertigo_pipe')
    vertigo_conf['mc_dir'] = conf.get('mc_dir', '/home/docker_device/vertigo/scopes')
    vertigo_conf['cache_dir'] = conf.get('cache_dir', '/home/docker_device/cache/scopes')
    vertigo_conf['mc_container'] = conf.get('mc_container',
                                            'micro_controller')
    vertigo_conf['mc_dependency'] = conf.get('mc_dependency', 'dependency')

    ''' Load storlet parameters '''
    configParser = ConfigParser.RawConfigParser()
    configParser.read(conf.get('__file__'))
    storlet_parameters = configParser.items('filter:storlet_handler')
    for key, val in storlet_parameters:
        vertigo_conf[key] = val
    
    configParser = ConfigParser.RawConfigParser()
    configParser.read(vertigo_conf['storlet_gateway_conf'])
    additional_items = configParser.items("DEFAULT")
    for key, val in additional_items:
        vertigo_conf[key] = val

    def swift_vertigo(app):
        return VertigoHandlerMiddleware(app, global_conf, vertigo_conf)

    return swift_vertigo
