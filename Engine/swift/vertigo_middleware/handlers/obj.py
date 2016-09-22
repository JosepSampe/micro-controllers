from swift.common.swob import HTTPMethodNotAllowed, Response
from swift.common.utils import public
from vertigo_middleware.handlers import VertigoBaseHandler
from vertigo_middleware.common.utils import get_microcontroller_list
from vertigo_middleware.common.utils import set_microcontroller
from vertigo_middleware.common.utils import delete_microcontroller
import time


class VertigoObjectHandler(VertigoBaseHandler):

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

    @property
    def is_copy_request(self):
        """
        Determines from a GET request if is a copy request
        """
        return 'X-Copy-From' in self.request.headers

    @property
    def is_mc_enabled(self):
        return self.request.headers['mc-enabled'] == 'True'

    @property
    def is_valid_request(self):
        """
        Determines if is a Vertigo valid request
        """
        return not self.is_copy_request and not self.is_slo_get_request \
            and self.is_mc_enabled and not self.is_vertigo_container_request

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
        Processes the data returned from the microcontroller
        """
        if mc_data['command'] == 'CONTINUE':
            return response

        elif mc_data['command'] == 'STORLET':
            slist = mc_data['list']
            self.logger.info('Vertigo - Go to execute Storlets: ' + str(slist))
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

        start = time.time()

        mc_list = get_microcontroller_list(response.headers, self.method)
        if mc_list:
            self.logger.info('Vertigo - There are microcontrollers' +
                             ' to execute: ' + str(mc_list))
            self._setup_docker_gateway(response)
            mc_data = self.mc_docker_gateway.execute_microcontrollers(mc_list)
            response = self._process_mc_data(response, mc_data)
        else:
            self.logger.info('Vertigo - No Microcontrollers to execute')

        end = time.time() - start
        self.logger.info('---')
        self.logger.info('Total execution time: ' +
                         str(int(round(end * 1000))) + 'ms')
        self.logger.info('---')
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
            trigger, micro_controller = self.get_mc_assignation_data()

            try:
                set_microcontroller(self, trigger, micro_controller)
                msg = 'Vertigo - Microcontroller "' + micro_controller + \
                    '" correctly assigned to the "' + trigger + '" trigger.\n'
            except ValueError as e:
                msg = e.args[0]
            self.logger.info(msg)

            response = Response(body=msg, headers={'etag': ''},
                                request=self.request)

        elif self.is_trigger_deletion:
            trigger, micro_controller = self.get_mc_deletion_data()

            try:
                delete_microcontroller(self, trigger, micro_controller)
                msg = 'Vertigo - Microcontroller "' + micro_controller +\
                    '" correctly removed from the "' + trigger + '" trigger.\n'
            except ValueError as e:
                msg = e.args[0]

            response = Response(body=msg, headers={'etag': ''},
                                request=self.request)
        else:
            response = self.request.get_response(self.app)

        return response
