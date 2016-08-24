from swift.common.swob import Request
from swift.common.swob import HTTPUnauthorized
from vertigo_middleware.common.utils import make_swift_request
import json


class VertigoGatewayStorlet():

    def __init__(self, conf, logger, app, api_v, account, method):
        self.conf = conf
        self.logger = logger
        self.app = app
        self.api_version = api_v
        self.account = account
        self.method = method
        self.server = self.conf['execution_server']

        self.storlet_container = self.conf["storlet_container"]
        self.storlet_metadata = None
        self.storlet_name = None
        self.gateway_module = self.conf['storlet_gateway_module']
        self.gateway_docker = None
        self.gateway_method = None
   
    def _get_storlet_data(self, storlet_data):
        storlet = storlet_data["storlet"]
        parameters = storlet_data["params"]
        server = storlet_data["server"]

        return storlet, parameters, server

    def _augment_storlet_request(self, req):
        """
        Add to request the storlet parameters to be used in case the request
        is forwarded to the data node (GET case)
        :param params: paramegers to be augmented to request
        """
        for key, val in self.storlet_metadata.iteritems():
            req.headers['X-Storlet-' + key] = val
            
    def _parse_storlet_params(self, headers):
        """
        Parse storlet parameters from storlet/dependency object metadata
        :returns: dict of storlet parameters
        """
        params = dict()
        for key in headers:
            if key.startswith('X-Object-Meta-Storlet'):
                params[key[len('X-Object-Meta-Storlet-'):]] = headers[key]
        return params
    
    def _verify_access_to_storlet(self, storlet):
        """
        Verify access to the storlet object
        :params storlet: storlet name
        :return: is accessible
        :raises HTTPUnauthorized: If it fails to verify access
        """
        spath = '/'.join(['', self.api_version, self.account,
                          self.storlet_container, storlet])
        self.logger.debug('Verify access to %s' % spath)
        
        resp = make_swift_request("HEAD", self.account,
                                  self.storlet_container,
                                  storlet)

        if not resp.is_success:
            return False

        self.storlet_name = storlet
        self.storlet_metadata = self._parse_storlet_params(resp.headers)
        for key in ['Content-Length', 'X-Timestamp']:
            self.storlet_metadata[key] = resp.headers[key]

        return True

    def _set_storlet_request(self, req_resp, params):

        self.gateway_docker = self.gateway_module(self.conf, self.logger, 
                                                  self.app, self.account)

        self.gateway_method = getattr(self.gateway_docker, "gateway" +
                                      self.server.title() +
                                      self.method.title() + "Flow")
        
        """ Simulate Storlet request """
        new_env = dict(req_resp.environ)
        sreq = Request.blank(new_env['PATH_INFO'], new_env)

        sreq.headers['X-Run-Storlet'] = self.storlet_name
        self._augment_storlet_request(sreq)
        sreq.environ['QUERY_STRING'] = params

        return sreq

    def _run_storlet(self, req_resp, params, vertigo_iter):
        sreq = self._set_storlet_request(req_resp, params)

        if self.method == 'PUT':
            sresp = self.gateway_method(sreq, vertigo_iter)
        elif self.method == 'GET':
            sresp = self.gateway_method(sreq, req_resp, vertigo_iter)
                
        return sresp.data_iter

    def execute_storlets(self, req_resp, storlet_list):
        vertigo_iter = None
        on_other_server = {}

        # Execute multiple Storlets, PIPELINE, if any.
        for key in sorted(storlet_list):
            storlet, params, server = self._get_storlet_data(storlet_list[key])
            
            if server == self.server:
                self.logger.info('Vertigo - Go to execute ' + storlet +
                                 ' storlet with parameters "' + params + '"')
                
                if not self._verify_access_to_storlet(storlet):
                    return HTTPUnauthorized('Vertigo - Storlet '+storlet+': No permission')
                
                vertigo_iter = self._run_storlet(req_resp, params, vertigo_iter)
                # Notify to the Proxy that Storlet was executed in the
                # object-server
                req_resp.headers["Storlet-Executed"] = True

            else:
                storlet_execution = {'storlet': storlet,
                                     'params': params,
                                     'server': server}
                launch_key = len(on_other_server.keys())
                on_other_server[launch_key] = storlet_execution

        if on_other_server:
            req_resp.headers['Storlet-List'] = json.dumps(on_other_server)

        if 'Storlet-Executed' in req_resp.headers:
            if isinstance(req_resp, Request):
                req_resp.environ['wsgi.input'] = vertigo_iter
            else:
                req_resp.app_iter = vertigo_iter

        return req_resp
