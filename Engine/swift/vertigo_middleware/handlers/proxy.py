from vertigo_middleware.handlers import VertigoBaseHandler
from vertigo_middleware.handlers.base import DEFAULT_MD_STRING, SYSMETA_OBJ_HEADER, \
    MICROCONTROLLERS_OBJ_HEADER, SYSMETA_CONT_HEADER, MICROCONTROLLERS_CONT_HEADER
from swift.common.swob import HTTPMethodNotAllowed, HTTPNotFound, HTTPUnauthorized, Response
from swift.common.utils import public, cache_from_env
from swift.common.wsgi import make_subrequest
import pickle
import redis
import json
import os


class VertigoProxyHandler(VertigoBaseHandler):

    def __init__(self, request, conf, app, logger):
        super(VertigoProxyHandler, self).__init__(
            request, conf, app, logger)

        self.mc_container = self.conf["mc_container"]
        self.memcache = None
        self.request.headers['mc-enabled'] = True
        self.memcache = cache_from_env(self.request.environ)

    def _parse_vaco(self):
        return self.request.split_path(3, 4, rest_with_last=True)

    def _is_object_in_cache(self, obj):
        """
        Checks if an object is in memcache. If exists, the object is stored
        in self.cached_object.
        :return: True/False
        """
        self.logger.info('Vertigo - Checking in cache: ' + obj)
        # self.cached_object = self.memcache.get(obj)

        self.cached_object = None

        return self.cached_object is not None

    def _get_cached_object(self, obj):
        """
        Gets the object from memcache. Executes associated microcontrollers.
        :return: Response object
        """
        self.logger.info('Vertigo - Object %s in cache', obj)
        cached_obj = pickle.loads(self.cached_object)
        resp_headers = cached_obj["Headers"]
        resp_headers['content-length'] = len(cached_obj["Body"])

        response = Response(body=cached_obj["Body"],
                            headers=resp_headers,
                            request=self.request)

        # TODO: Execute microcontrollers
        return response

    def handle_request(self):
        if hasattr(self, self.request.method) and self.is_valid_request:
            try:
                handler = getattr(self, self.request.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                return HTTPMethodNotAllowed(request=self.request)
            return handler()
        else:
            return self.request.get_response(self.app)
            # return HTTPMethodNotAllowed(request=self.request)

    def _augment_empty_request(self):
        """
        Auxiliary function that sets the content-length header and the body
        of the request in such cases that the user doesn't send the metadata
        file when he assign a microcontroller to an object.
        """
        if 'Content-Length' not in self.request.headers:
            self.request.headers['Content-Length'] = 0
            self.request.body = ''

    def _verify_access(self, cont, obj):
        """
        Verifies access to the specified object in swift
        :param cont: swift container name
        :param obj: swift object name
        :raise HTTPNotFound: if the object doesn't exists in swift
        :return md: Object metadata
        """
        path = os.path.join('/', self.api_version, self.account, cont, obj)
        self.logger.debug(' Verifying access to %s' % path)

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
        response = sub_req.get_response(self.app)

        if not response.is_success:
            if response.status_int == 401:
                raise HTTPUnauthorized('Unauthorized to access to this '
                                       'resource: ' + cont + '/' + obj + '\n')
            else:
                raise HTTPNotFound('Vertigo - Object error: "' + cont + '/' +
                                   obj + '" doesn\'t exists in Swift.\n')
        else:
            return response

    def _augment_object_list(self, obj_list):
        """
        Checks the object list and creates those pseudo-folders that are not in
        the obj_list, but there are objects within them.
         :param obj_list: object list
        """
        for obj in obj_list:
            if '/' in obj:
                obj_split = obj.rsplit('/', 1)
                pseudo_folder = obj_split[0] + '/'
                if pseudo_folder not in obj_list:
                    path = os.path.join('/', self.api_version, self.account,
                                        self.container, pseudo_folder)
                    new_env = dict(self.request.environ)
                    auth_token = self.request.headers.get('X-Auth-Token')
                    sub_req = make_subrequest(new_env, 'PUT', path,
                                              headers={'X-Auth-Token': auth_token,
                                                       'Content-Length': 0},
                                              swift_source='Vertigo')
                    response = sub_req.get_response(self.app)
                    if response.is_success:
                        obj_list.append(pseudo_folder)
                    else:
                        raise ValueError("Vertigo - Error creating pseudo-folder")

    def _get_object_list(self, path):
        """
        Gets an object list of a specified path. The path may be '*', that means
        it returns all objects inside the container or a pseudo-folder.
        :param path: pseudo-folder path (ended with *), or '*'
        :return: list of objects
        """
        obj_list = list()

        dest_path = os.path.join('/', self.api_version, self.account, self.container)
        new_env = dict(self.request.environ)
        auth_token = self.request.headers.get('X-Auth-Token')

        if path == '*':
            # All objects inside a container hierarchy
            obj_list.append('')
        else:
            # All objects inside a pseudo-folder hierarchy
            obj_split = self.obj.rsplit('/', 1)
            pseudo_folder = obj_split[0] + '/'
            new_env['QUERY_STRING'] = 'prefix='+pseudo_folder

        sub_req = make_subrequest(new_env, 'GET', dest_path,
                                  headers={'X-Auth-Token': auth_token},
                                  swift_source='Vertigo')
        response = sub_req.get_response(self.app)
        for obj in response.body.split('\n'):
            if obj != '':
                obj_list.append(obj)

        self._augment_object_list(obj_list)

        return obj_list

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

    def _get_parent_mc_metadata(self):
        """
        Makes a HEAD to the parent pseudo-folder or container (7ms overhead)
        in order to get the associated micro-controllers.
        :return: vertigo metadata dictionary
        """
        obj_split = self.obj.rsplit('/', 1)

        if len(obj_split) > 1:
            # object parent is pseudo-foldder
            psudo_folder = obj_split[0] + '/'
            mc_key = 'X-Object-Sysmeta-Vertigo-Microcontroller'
            dest_path = os.path.join('/', self.api_version, self.account, self.container, psudo_folder)
        else:
            # object parent is container
            mc_key = 'X-Container-Sysmeta-Vertigo-Microcontroller'
            dest_path = os.path.join('/', self.api_version, self.account, self.container)

        vertigo_metadata = self.memcache.get("vertigo_"+dest_path)
        if vertigo_metadata:
            """
            We first try to get the micro-controller execution list from memcache
            """
            for key in vertigo_metadata.keys():
                if key.replace('Container', 'Object').startswith('X-Object-Sysmeta-Vertigo-'):
                    if key == mc_key:
                        mc = eval(vertigo_metadata.pop(key))
                        vertigo_metadata[key.replace('Container', 'Object')] = mc
                    else:
                        vertigo_metadata[key.replace('Container', 'Object')] = vertigo_metadata.pop(key)
        else:
            """
            If the micro-controller execution list is not in memcache, we get it from Swift
            """
            new_env = dict(self.request.environ)
            auth_token = self.request.headers.get('X-Auth-Token')
            sub_req = make_subrequest(new_env, 'HEAD', dest_path,
                                      headers={'X-Auth-Token': auth_token},
                                      swift_source='Vertigo')
            response = sub_req.get_response(self.app)

            vertigo_metadata = dict()
            if response.is_success:
                for key in response.headers:
                    if key.replace('Container', 'Object').startswith('X-Object-Sysmeta-Vertigo-'):
                        # if key.replace('Container', 'Object').startswith('X-Object-Sysmeta-Vertigo-Onput'):
                        #    continue
                        if key == mc_key:
                            mc = eval(response.headers[key])
                            # del mc['onput']
                            vertigo_metadata[key.replace('Container', 'Object')] = mc
                        else:
                            vertigo_metadata[key.replace('Container', 'Object')] = response.headers[key]
            self.memcache.set("vertigo_"+dest_path, vertigo_metadata)

        return vertigo_metadata

    def _process_mc_assignation_deletion_request(self):
        """
        Process both mc-trigger assignation and trigger deletion over an object
        or a group of objects
        """
        obj_list = list()
        if self.is_trigger_assignation:
            trigger, micro_controller = self.get_mc_assignation_data()
            self._verify_access(self.mc_container, micro_controller)

        if '*' in self.obj:
            obj_list = self._get_object_list(self.obj)
        else:
            obj_list.append(self.obj)

        if self.obj == '*':
            # Save micro-controller information into container metadata
            if self.is_trigger_assignation:
                self.set_microcontroller_container(trigger, micro_controller)
            elif self.is_trigger_deletion:
                trigger, micro_controller = self.get_mc_deletion_data()
                self.delete_microcontroller_container(trigger, micro_controller)

        for obj in obj_list:
            if self.is_trigger_assignation:
                try:
                    self.set_microcontroller_object(trigger, micro_controller, obj)
                    msg = 'Micro-controller "' + micro_controller + \
                        '" correctly assigned to the "' + trigger + '" trigger.\n'
                except ValueError as e:
                    msg = e.args[0]

                self.logger.info(msg)
                response = Response(body=msg, headers={'etag': ''},
                                    request=self.request)

            elif self.is_trigger_deletion:
                trigger, micro_controller = self.get_mc_deletion_data()
                try:
                    self.delete_microcontroller_object(trigger, micro_controller, obj)
                    msg = 'Micro-controller "' + micro_controller +\
                        '" correctly removed from the "' + trigger + '" trigger.\n'
                except ValueError as e:
                    msg = e.args[0]

                self.logger.info(msg)
                response = Response(body=msg, headers={'etag': ''},
                                    request=self.request)

        return response

    def _check_microcntroller_execution(self, obj):
        user_agent = self.request.headers['User-Agent']
        if user_agent == "vertigo/microcontroller":
            req_token = self.request.headers['X-Vertigo-Token']
            key = 'VERTIGO_TOKEN_' + req_token.split('-')[0] + "_" + obj
            admin_token = self.memcache.get(key)
            req_token = self.request.headers['X-Vertigo-Token']
            if req_token == admin_token:
                self.request.headers['mc-enabled'] = False
                self.logger.info('Vertigo - Microcontroller execution disabled'
                                 ': Request from microcontroller')

    def _process_mc_data(self, mc_data):
        """
        Processes the data returned from the microcontroller
        """
        if mc_data['command'] == 'CONTINUE':
            return self.request.get_response(self.app)

        elif mc_data['command'] == 'STORLET':
            slist = mc_data['list']
            self.logger.info('Vertigo - Going to execute Storlets: ' + str(slist))
            return self.apply_storlet_on_put(self.request, slist)

        elif mc_data['command'] == 'CANCEL':
            msg = mc_data['message']
            return Response(body=msg + '\n', headers={'etag': ''},
                            request=self.request)

    @public
    def GET(self):
        """
        GET handler on Proxy
        """
        obj = os.path.join(self.account, self.container, self.obj)
        # self._check_microcntroller_execution(obj)

        if self._is_object_in_cache(obj):
            response = self._get_cached_object(obj)
        else:
            response = self.request.get_response(self.app)

        if response.headers['Content-Type'] == 'vertigo/link':
            dest_obj = response.headers['X-Object-Sysmeta-Vertigo-Link-to']
            obj = os.path.join(self.account, dest_obj)
            if self._is_object_in_cache(obj):
                response = self._get_cached_object(obj)
            else:
                response = self._get_linked_object(dest_obj)

        if 'Storlet-List' in response.headers and \
                self.is_account_storlet_enabled():
            self.logger.info('Vertigo - There are Storlets to execute')
            storlet_list = json.loads(response.headers.pop('Storlet-List'))
            response = self.apply_storlet_on_get(response, storlet_list)

        if 'Content-Length' not in response.headers:
            response.headers['Content-Length'] = None
            if 'Transfer-Encoding' in response.headers:
                response.headers.pop('Transfer-Encoding')

        return response

    @public
    def PUT(self):
        """
        PUT handler on Proxy
        """
        # When a users puts an object, the micro-controllers assigned to the
        # parent container or pseudo-folder are assigned by default to
        # the new object. Onput micro-controllers are executed here.
        # start = time.time()
        mc_metadata = self._get_parent_mc_metadata()
        self.request.headers.update(mc_metadata)
        mc_list = self.get_microcontroller_list_object(mc_metadata, self.method)
        if mc_list:
            self.logger.info('Vertigo - There are microcontrollers' +
                             ' to execute: ' + str(mc_list))
            self._setup_docker_gateway()
            mc_data = self.mc_docker_gateway.execute_microcontrollers(mc_list)
            # end = time.time() - start
            response = self._process_mc_data(mc_data)
            # f = open("/tmp/vertigo/vertigo_put_overhead.log", 'a')
            # f.write(str(int(round(end * 1000)))+'\n')
            # f.close()
        else:
            response = self.request.get_response(self.app)

        return response

    @public
    def POST(self):
        """
        POST handler on Proxy
        """
        if self.is_trigger_assignation or self.is_trigger_deletion:
            response = self._process_mc_assignation_deletion_request()
        else:
            obj_metadata = self.get_object_metadata(self.obj)

            for key in obj_metadata:
                if key.startswith(MICROCONTROLLERS_OBJ_HEADER) or \
                   key.startswith(MICROCONTROLLERS_CONT_HEADER):
                    self.request.headers[key] = obj_metadata[key]

            response = self.request.get_response(self.app)

        return response

    @public
    def HEAD(self):
        """
        HEAD handler on Proxy
        """
        response = self.request.get_response(self.app)

        if self.conf['metadata_visibility']:
            for key in response.headers.keys():
                if key.replace('Container', 'Object').startswith(MICROCONTROLLERS_OBJ_HEADER):
                    new_key = key.replace('Container', 'Object').replace(SYSMETA_OBJ_HEADER, '')
                    response.headers[new_key] = response.headers[key]

            if 'Microcontrollers-list' in response.headers:
                mc_dict = eval(response.headers.pop('Microcontrollers-list'))
                for trigger in mc_dict.keys():
                    if not mc_dict[trigger]:
                        del mc_dict[trigger]
                response.headers['Micro-controllers List'] = mc_dict

        return response
