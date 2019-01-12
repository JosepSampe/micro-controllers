from swiftclient import client as c
import os


def put_mc_object(url, token, handler_name, handler_path, main_class, dependency):

    metadata = {'X-Object-Meta-Function-Language': 'Java',
                'X-Object-Meta-Function-Interface-Version': '1.0',
                'X-Object-Meta-Function-Library-Dependency': dependency,
                'X-Object-Meta-Function-Main': main_class}
    f = open('%s/%s' % (handler_path, handler_name), 'r')
    content_length = os.stat(handler_path+'/'+handler_name).st_size
    response = dict()

    c.put_object(url, token, 'function', handler_name, f,
                 content_length, None, None,
                 "application/octet-stream", metadata,
                 None, None, None, response)
    f.close()
    status = response.get('status')
    assert (status == 200 or status == 201)


def put_mc_dependency(url, token, dep_name, local_path_to_dep):
    metadata = {'X-Object-Meta-Function-Dependency-Version': '1'}
    f = open('%s/%s' % (local_path_to_dep, dep_name), 'r')
    content_length = None
    response = dict()
    c.put_object(url, token, 'dependency', dep_name, f,
                 content_length, None, None, "application/octet-stream",
                 metadata, None, None, None, response)
    f.close()
    status = response.get('status')
    assert (status == 200 or status == 201)


AUTH_PORT = '5000'
AUTH_IP = 'iostack.urv.cat'
ACCOUNT = 'josep'
USER_NAME = 'josep'
PASSWORD = 'jsampe'

os_options = {'tenant_name': ACCOUNT}

url, token = c.get_auth("http://" + AUTH_IP + ":" + AUTH_PORT + "/v2.0", ACCOUNT + ":"+USER_NAME, PASSWORD, os_options=os_options, auth_version="2.0")

"""-------------------------------------------------------------------------------------------"""
print url, token

# Ratio
# put_mc_object(url, token, 'ratio-1.0.jar', '../FunctionSamples/Ratio/bin', 'com.urv.vertigo.function.ratio.Handler', '')

# Prefetching
put_mc_object(url, token, 'prefetching-1.0.jar', '../FunctionSamples/Prefetching/bin', 'com.urv.vertigo.function.prefetching.Handler', '')

# LIMITER
# put_mc_object(url, token, 'limiter-1.0.jar', '../FunctionSamples/Limiter/bin', 'com.urv.vertigo.function.limiter.Handler', '')

# NOOP DATA ITERATOR
# put_mc_object(url, token, 'noop-data-iterator-1.0.jar', '../FunctionSamples/NoopDataIterator/bin', 'com.urv.vertigo.function.noopdataiterator.Handler', '')

# TransGrep
# put_mc_object(url, token, 'transgrep-1.0.jar', '../FunctionSamples/TransGrep/bin', 'com.urv.vertigo.function.transgrep.Handler', '')

# CBAC
# put_mc_object(url, token, 'cbac-1.0.jar', '../FunctionSamples/ContentBasedAccessControl/bin', 'com.urv.vertigo.function.cbac.Handler', '')
