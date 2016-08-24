from vertigo_middleware.handlers import VertigoBaseHandler
from vertigo_middleware.common.utils import verify_access, create_link
from swift.common.swob import HTTPMethodNotAllowed, HTTPNotFound, Response
from swift.common.utils import public, cache_from_env
from swift.common.wsgi import make_subrequest
import pickle
import json
import os


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
            #return HTTPMethodNotAllowed(request=self.request)
         
    def _augment_empty_request(self):
        """
        Auxiliary function that sets the content-length header and the body
        of the request in such cases that the user doesn't send the metadata
        file when he assign a microcontroller to an object.
        """
        if not 'Content-Length' in self.request.headers:
            self.request.headers['Content-Length'] = 0
            self.request.body = ''
  
    def _verify_access(self, container, obj):
        """
        Verifies acces to the specified object in swift
        :param container: swift container name
        :param obj: swift object name
        :raise HTTPNotFound: if the object doesn't exists in swift 
        """
        path = os.path.join('/', self.api_version, self.account, container, obj)
        md = verify_access(self, path)
        if not md:
            raise HTTPNotFound('Vertigo - Object error: "' + container + '/'
                               + obj + '" doesn\'t exists in Swift.\n')
        else:
            return md
        
    def _get_linked_object(self, dest_obj):
        """
        Makes a subrequest to the provided container/object
        :param dest_obj: container/object
        :return: swift.common.swob.Response Instance
        """
        dest_path = os.path.join('/', self.api_version, self.account, dest_obj)
        new_env = dict(self.request.environ)
        sub_req = make_subrequest(new_env, 'GET', dest_path,
                            headers=self.request.headers,
                            swift_source='Vertigo')

        return sub_req.get_response(self.app)
    
    def _call_storlet_gateway_on_put(self, req, storlet_list):
        req, app_iter = self.storlet_gateway.execute_storlets(req, storlet_list)
        req.environ['wsgi.input'] = app_iter
        return req

    def _call_storlet_gateway_on_get(self, resp, storlet_list):
        resp, app_iter = self.storlet_gateway.execute_storlets(
            resp, storlet_list)
        resp.app_iter = app_iter
        resp.headers.pop('Storlet-List')
        resp.headers.pop("Storlet-Executed")
        return resp

    @public
    def GET(self):
        """
        GET handler on Proxy
        """        
        if self.is_object_in_cache():
            value = pickle.loads(self.cached_object)

            self.logger.info('Vertigo - Object in cache')

            resp_headers = value["Headers"]
            resp_headers['content-length'] = len(value["Body"])

            return Response(body=value["Body"],
                            headers=resp_headers,
                            request=self.request)
        else:
            response = self.request.get_response(self.app)

        if response.headers['Content-Type'] == 'Vertigo-Link':
            dest_obj = response.headers['X-Object-Sysmeta-Vertigo-Link-to']
            response = self._get_linked_object(dest_obj)

        if 'Storlet-List' in response.headers and \
                self.is_account_storlet_enabled():
            # There are storlets to execute on proxy side
            self.logger.info('Vertigo - There are Storlets to execute')

            self._setup_storlet_gateway()
            storlet_list = json.loads(response.headers['Storlet-List'])
            response = self.apply_storlet_on_get(response, storlet_list)

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
            _, micro_controller = self.get_mc_assignation_data()
            self._verify_access(self.container, self.obj)
            self._verify_access(self.mc_container, micro_controller)
            self._augment_empty_request()
            response = self.request.get_response(self.app)
        
        elif self.is_trigger_deletion:
            self._verify_access(self.container, self.obj)
            self._augment_empty_request()
            response = self.request.get_response(self.app)

        elif self.is_object_move:
            link_path = os.path.join('/', self.api_version, self.account, 
                                     self.container, self.obj)
            dest_path = self.request.headers['X-Vertigo-Link-To']
            link_md = self._verify_access(self.container, self.obj) 
            if "X-Object-Sysmeta-Vertigo-Link-to" not in link_md \
                    and link_md['Content-Type'] != 'Vertigo-Link':
                self.request.method = 'COPY'
                self.request.headers['Destination'] = dest_path
                response = self.request.get_response(self.app)

            response = create_link(self, link_path, dest_path)
        else:
            response = self.request.get_response(self.app)
            
        return response
    
    @public
    def POST(self):
        """
        POST handler on Proxy
        """
        if self.is_trigger_assignation:
            _, micro_controller = self.get_mc_assignation_data()
            self._verify_access(self.container, self.obj)
            self._verify_access(self.mc_container, micro_controller)
            # Converts the POST request to a PUT request to properly forward
            # it to the object server.
            self.request.method = 'PUT'
            self._augment_empty_request()
           
        if self.is_trigger_deletion:
            self._verify_access(self.container, self.obj)
            # Converts the POST request to a PUT request to properly forward
            # it to the object server.
            self.request.method = 'PUT'
            self._augment_empty_request()

        return self.request.get_response(self.app)

    @public
    def HEAD(self):
        """
        HEAD handler on Proxy
        """
        response = self.request.get_response(self.app)

        for key in response.headers.keys():
            if key.startswith('X-Object-Sysmeta-Vertigo-'):
                response.headers[key.replace('X-Object-Sysmeta-','')] = response.headers[key]
        
        if 'Vertigo-Microcontroller' in response.headers:
            mc_dict = eval(response.headers['Vertigo-Microcontroller'])
            for trigger in mc_dict.keys():
                if not mc_dict[trigger]:
                    del mc_dict[trigger]
            response.headers['Vertigo-Microcontroller'] = mc_dict

        return response
    
    @public
    def MOVE(self):
        """
        MOVE handler on Proxy
        """
        link_path = os.path.join(self.container, self.obj)
        dest_path = self.request.headers['Destination']
        
        link_md = self._verify_access(self.container, self.obj) 
     
        if "X-Object-Sysmeta-Vertigo-Link-to" not in link_md \
                    and link_md['Content-Type'] != 'Vertigo-Link':
            self.request.method = 'COPY'
            response = self.request.get_response(self.app)
            
        response = create_link(self, link_path, dest_path)
        
        return response
