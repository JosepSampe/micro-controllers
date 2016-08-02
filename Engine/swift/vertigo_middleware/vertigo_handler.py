from swift.common.swob import HTTPInternalServerError, HTTPException, wsgify
from swift.common.utils import get_logger
from ConfigParser import RawConfigParser

from vertigo_middleware.handlers import VertigoProxyHandler, VertigoObjectHandler
from vertigo_middleware.handlers.base import NotVertigoRequest


class VertigoHandlerMiddleware(object):

    def __init__(self, app, conf, vertigo_conf):
        self.app = app
        self.exec_server = vertigo_conf.get('execution_server')
        self.logger = get_logger(conf, log_route='vertigo_handler')
        self.vertigo_conf = vertigo_conf
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
    vertigo_conf['ic_pipe'] = conf.get('mc_pipe', 'internal_client_pipe')
    vertigo_conf['mc_dir'] = conf.get('mc_dir', '/home/docker_device/vertigo/scopes')
    vertigo_conf['cache_dir'] = conf.get('cache_dir', '/home/docker_device/cache/scopes')
    vertigo_conf['mc_container'] = conf.get('mc_container',
                                            'micro_controller')
    vertigo_conf['mc_dependency'] = conf.get('mc_dependency', 'dependency')

    ''' Load storlet parameters '''
    configParser = RawConfigParser()
    configParser.read(conf.get('__file__'))
    storlet_parameters = configParser.items('filter:storlet_handler')
    for key, val in storlet_parameters:
        vertigo_conf[key] = val
    
    configParser = RawConfigParser()
    configParser.read(vertigo_conf['storlet_gateway_conf'])
    additional_items = configParser.items("DEFAULT")
    for key, val in additional_items:
        vertigo_conf[key] = val

    def swift_vertigo(app):
        return VertigoHandlerMiddleware(app, global_conf, vertigo_conf)

    return swift_vertigo
