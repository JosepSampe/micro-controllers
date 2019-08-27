from swift.common.swob import HTTPMethodNotAllowed, Response
from swift.common.utils import public
from vertigo_middleware.handlers import VertigoBaseHandler
from vertigo_middleware.handlers.base import MICROCONTROLLERS_OBJ_HEADER, MICROCONTROLLERS_LIST_OBJ_HEADER


class VertigoObjectHandler(VertigoBaseHandler):

    def __init__(self, request, conf, app, logger):
        super(VertigoObjectHandler, self).__init__(
            request, conf, app, logger)

    def _parse_vaco(self):
        _, _, acc, cont, obj = self.request.split_path(
            5, 5, rest_with_last=True)
        return ('v1', acc, cont, obj)

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

    def _process_mc_data(self, response, mc_data):
        """
        Processes the data returned from the micro-controller
        """
        if mc_data['command'] == 'CONTINUE':
            return response

        elif mc_data['command'] == 'STORLET':
            slist = mc_data['list']
            self.logger.info('Going to execute Storlets: ' + str(slist))
            return self.apply_storlet_on_get(response, slist)

        elif mc_data['command'] == 'CANCEL':
            msg = mc_data['message']
            return Response(body=msg + '\n', headers={'etag': ''},
                            request=self.request)

    @public
    def GET(self):
        """
        GET handler on Object
        """
        response = self.request.get_response(self.app)

        # start = time.time()
        response.headers[MICROCONTROLLERS_LIST_OBJ_HEADER] = {'onget': 'OCEmc.jar'}
        response.headers['Dynamic-Policies'] = self.request.headers['Dynamic-Policies']

        if self.obj.endswith('/'):
            # is a pseudo-folder
            mc_list = None
        else:
            mc_list = self.get_microcontroller_list_object(response.headers, self.method)

        if mc_list:
            self.logger.info('Vertigo - There are micro-controllers' +
                             ' to execute: ' + str(mc_list))
            self._setup_docker_gateway(response)
            mc_data = self.mc_docker_gateway.execute_microcontrollers(mc_list)
            response = self._process_mc_data(response, mc_data)
        else:
            self.logger.info('Vertigo - No micro-controllers to execute')

        # end = time.time() - start
        # f = open("/tmp/vertigo/vertigo_get_overhead.log", 'a')
        # f.write(str(int(round(end * 1000)))+'\n')
        # f.close()

        return response
