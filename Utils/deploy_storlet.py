from swiftclient import client as c


def enable_account_for_storlets(url, token):
    headers = dict()
    headers['X-Account-Meta-storlet-enabled'] = 'True'
    c.post_account(url, token, headers)


def put_storlet_object(url, token, storlet_path, storlet_name, main_class, dependency=''):

    metadata = {'X-Object-Meta-Storlet-Language': 'Java',
                'X-Object-Meta-Storlet-Interface-Version': '1.0',
                'X-Object-Meta-Storlet-Dependency': dependency,
                'X-Object-Meta-Storlet-Object-Metadata': 'no',
                'X-Object-Meta-Storlet-Main': main_class}

    f = open('%s/%s' % (storlet_path, storlet_name), 'r')
    content_length = None
    response = dict()

    c.put_object(url, token, 'storlet', storlet_name, f,
                 content_length, None, None,
                 "application/octet-stream", metadata,
                 None, None, None, response)
    f.close()
    status = response.get('status')
    assert (status == 200 or status == 201)


def put_storlet_dependency(url, token, local_path_to_dep, dep_name):
    metadata = {'X-Object-Meta-Storlet-Dependency-Version': '1'}
    f = open('%s/%s' % (local_path_to_dep, dep_name), 'r')
    content_length = None
    response = dict()
    c.put_object(url, token, 'dependency', dep_name, f,
                 content_length, None, None, "application/octet-stream",
                 metadata, None, None, None, response)
    f.close()
    status = response.get('status')
    assert (status == 200 or status == 201)


keystone_ip = '10.30.220.98'
keystone_url = 'http://{}:5000/v3'.format(keystone_ip)
ACCOUNT = 'vertigo'
USER_NAME = 'vertigo'
PASSWORD = 'vertigo'

url, token = c.get_auth(keystone_url, ACCOUNT + ":"+USER_NAME, PASSWORD, auth_version="3")
# print url, token


"""
 ------------------- Deploy Storlets to Swift Cluster -----------------
"""
path = '../StorletSamples'

# No-operation Storlet
put_storlet_object(url, token, path+'/Storlet_Noop/bin', 'noop-1.0.jar', 'com.urv.storlet.noop.NoopStorlet')

# Compression Storlet
put_storlet_object(url, token, path+'/Storlet_Compress/bin', 'compress-1.0.jar', 'com.urv.storlet.compress.CompressStorlet')

# Encryption Storlet
put_storlet_object(url, token, path+'/Storlet_Crypto/bin', 'crypto-1.0.jar', 'com.urv.storlet.crypto.AESEncryptionStorlet')

# UbuntuOne Trace Storlet (SQL Filter)
put_storlet_object(url, token, path+'/Storlet_UOneTrace/bin', 'UOneTrace-1.0.jar', 'com.urv.storlet.uonetrace.UOneTraceStorlet')

# Adaptative bandwith Storlet
put_storlet_object(url, token, path+'/Storlet_Adaptative/bin', 'adaptative-1.0.jar', 'com.urv.storlet.adaptative.AdaptativeStorlet')

# Adult dataset (csv) Storlet
put_storlet_object(url, token, path+'/Storlet_Adult/bin', 'adult-1.0.jar', 'com.urv.storlet.adult.AdultStorlet')

# Grep Storlet
put_storlet_object(url, token, path+'/Storlet_Grep/bin', 'grep-1.0.jar', 'com.urv.storlet.grep.GrepStorlet', 'commons-compress-1.6.jar,grep4j-1.8.7.jar')
put_storlet_dependency(url, token, path+'/Storlet_Grep/lib', 'commons-compress-1.6.jar')
put_storlet_dependency(url, token, path+'/Storlet_Grep/lib', 'grep4j-1.8.7.jar')

# HTML parser Storlet
put_storlet_object(url, token, path+'/Storlet_ScanHtml/bin', 'ScanHtml-1.0.jar', 'com.urv.storlet.scanhtml.ScanHtml', 'commons-compress-1.6.jar,jsoup-1.8.3.jar')
put_storlet_dependency(url, token, path+'/Storlet_ScanHtml/lib', 'jsoup-1.8.3.jar')

# Blurfaces Storlet
put_storlet_object(url, token, path+'/Storlet_BlurFaces/bin', 'blurfaces-1.0.jar', 'com.ibm.storlet.blurfaces.BlurFacesStorlet', 'commons-compress-1.2.jar,blur_faces_all.tar.gz')
put_storlet_dependency(url, token, path+'/Storlet_BlurFaces/lib', 'commons-compress-1.6.jar')
put_storlet_dependency(url, token, path+'/Storlet_BlurFaces/lib', 'blur_faces_all.tar.gz')

# Watermark Storlet
put_storlet_object(url, token, path+'/Storlet_Watermark/bin', 'watermark-1.0.jar', 'com.urv.storlet.watermark.WatermarkStorlet', 'commons-compress-1.2.jar,commons-io-1.3.2.jar,ffmpeg')
put_storlet_dependency(url, token, path+'/Storlet_Watermark/lib', 'commons-io-1.3.2.jar')
put_storlet_dependency(url, token, path+'/Storlet_Watermark/lib', 'ffmpeg')

# Transcoder Storlet
# put_storlet_object(url, token, path+'/Storlet_Transcoder/bin', 'transcoder-1.0.jar', 'com.ibm.storlet.transcoder.TranscoderStorlet', 'commons-logging-1.1.3.jar,fontbox-1.8.4.jar,jempbox-1.8.4.jar,pdfbox-app-1.8.4.jar')
# put_storlet_dependency(url, token, path+'/Storlet_Transcoder/lib', 'commons-logging-1.1.3.jar')
# put_storlet_dependency(url, token, path+'/Storlet_Transcoder/lib', 'fontbox-1.8.4.jar')
# put_storlet_dependency(url, token, path+'/Storlet_Transcoder/lib', 'jempbox-1.8.4.jar')
# put_storlet_dependency(url, token, path+'/Storlet_Transcoder/lib', 'pdfbox-app-1.8.4.jar')

print('Done!')
