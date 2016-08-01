from swift.common.swob import HTTPMethodNotAllowed, HTTPUnauthorized, Response
from swift.common.utils import public, cache_from_env
import pickle
import json

from vertigo_middleware.handlers import VertigoBaseHandler
from vertigo_middleware.common.utils import verify_access


class VertigoProxyHandler(VertigoBaseHandler):

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
            response = self.request.get_response(self.app)

        if 'Vertigo' in response.headers and \
                self.is_account_storlet_enabled():
            # There are storlets to execute on proxy side
            self.logger.info('Vertigo - There are Storlets to execute')

            self._setup_storlet_gateway()
            storlet_list = json.loads(response.headers['Vertigo'])
            return self.apply_storlet_on_get(response, storlet_list)

        # There are no storlets to execute on proxy side
        elif 'Storlet-Executed' in response.headers:
            #  Storlet was already invoked at object side
            if 'Transfer-Encoding' in response.headers:
                response.headers.pop('Transfer-Encoding')
            response.headers['Content-Length'] = None

        return response

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
