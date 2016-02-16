from swiftclient import client as c


'''------------------------------------------------------------------------'''
def enable_account_for_storlets(url,token):
    headers = dict()
    headers['X-Account-Meta-storlet-enabled'] = 'True'
    c.post_account(url, token, headers)
    
'''------------------------------------------------------------------------'''
def put_storlet_object(url, token, storlet_name, storlet_path, main_class, dependency):
    # Delete previous storlet
    #resp = dict()
    '''
    try:
        c.delete_object(url, token, 'storlet', storlet_name, None, 
                        None, None, None, resp)
    except Exception as e:
        if (resp.get('status')== 404):
            print 'Nothing to delete' 
    '''   
       
    metadata = {'X-Object-Meta-Storlet-Language':'Java',
                'X-Object-Meta-Storlet-Interface-Version':'1.0',
                'X-Object-Meta-Storlet-Dependency': dependency, 
                'X-Object-Meta-Storlet-Object-Metadata':'no',
                'X-Object-Meta-Storlet-Main': main_class}
    f = open('%s/%s' % (storlet_path, storlet_name),'r')
    content_length = None
    response = dict()

    c.put_object(url, token, 'storlet', storlet_name, f, 
                 content_length, None, None, 
                 "application/octet-stream", metadata, 
                 None, None, None, response)
    f.close()
    status = response.get('status') 
    assert (status == 200 or status == 201)

    
'''------------------------------------------------------------------------'''
def put_storlet_dependency(url, token, dep_name, local_path_to_dep):
    metadata = {'X-Object-Meta-Storlet-Dependency-Version': '1'}
    f = open('%s/%s' %(local_path_to_dep, dep_name),'r')
    content_length = None
    response = dict()
    c.put_object(url, token, 'dependency', dep_name, f, 
                 content_length, None, None, "application/octet-stream", 
                 metadata, None, None, None, response)
    f.close()
    status = response.get('status') 
    assert (status == 200 or status == 201)
    
    

AUTH_PORT = '5000'
AUTH_IP = '10.30.239.240'
ACCOUNT = 'service'
USER_NAME = 'swift'
PASSWORD = 'urv'

os_options = {'tenant_name': ACCOUNT}

url, token = c.get_auth("http://" + AUTH_IP + ":" + AUTH_PORT + "/v2.0", ACCOUNT +":"+USER_NAME, PASSWORD, os_options = os_options, auth_version="2.0")

"""-------------------------------------------------------------------------------------------"""
print token

#HTML PARSER
put_storlet_object(url, token,'ScanHtml-1.0.jar','/home/josep/Josep/workspace/Storlet_ScanHtml/bin' ,'com.urv.storlet.scanhtml.ScanHtml', 'commons-compress-1.6.jar,json-simple-1.1.1.jar,jsoup-1.8.3.jar')
#put_storlet_dependency(url, token,'commons-compress-1.6.jar','/home/josep/Josep/workspace/Storlet_ScanHtml/lib')
#put_storlet_dependency(url, token,'json-simple-1.1.1.jar','/home/josep/Josep/workspace/Storlet_ScanHtml/lib')
#put_storlet_dependency(url, token,'jsoup-1.8.3.jar','/home/josep/Josep/workspace/Storlet_ScanHtml/lib')

#UONETRACE
#put_storlet_object(url, token,'UOneTrace-1.0.jar','/home/josep/Josep/workspace/Storlet_UOneTrace/bin' ,'com.urv.storlet.uonetrace.UOneTraceStorlet', '')

#ADAPTATIVE
#put_storlet_object(url, token,'adaptative-1.0.jar','/home/josep/Josep/workspace/Storlet_Adaptative/bin' ,'com.urv.storlet.adaptative.AdaptativeStorlet', '')


#COMPRESSION
#put_storlet_object(url, token,'compress-1.0.jar','/home/josep/Josep/workspace/Storlet_Compress/bin' ,'com.urv.storlet.compress.CompressStorlet', 'commons-compress-1.2.jar,lz4-1.3.0.jar')
#put_storlet_dependency(url, token,'commons-compress-1.2.jar','/home/josep/Josep/workspace/Storlet_Compress/lib')
#put_storlet_dependency(url, token,'lz4-1.3.0.jar','/home/josep/Josep/workspace/Storlet_Compress/lib')

#NOTHING
#put_storlet_object(url, token,'nothing-1.0.jar','/home/josep/Josep/workspace/Storlet_Nothing/bin' ,'com.urv.storlet.nothing.NothingStorlet', 'commons-compress-1.6.jar')
#put_storlet_dependency(url, token,'commons-compress-1.6.jar','/home/josep/Josep/workspace/Storlet_Nothing/lib')
#put_storlet_dependency(url, token,'json-simple-1.1.1.jar','/home/josep/Josep/workspace/Storlet_Nothing/lib')

#Adult
#put_storlet_object(url, token,'adult-1.0.jar','/home/josep/Josep/workspace/Storlet_Adult/bin' ,'com.urv.storlet.adult.AdultStorlet', '')

#Grep
#put_storlet_object(url, token,'grep-1.0.jar','/home/josep/Josep/workspace/Storlet_Grep/bin' ,'com.urv.storlet.grep.GrepStorlet', 'commons-compress-1.6.jar,grep4j-with-dependencies.jar,json-simple-1.1.1.jar')
#put_storlet_dependency(url, token,'commons-compress-1.6.jar','/home/josep/Josep/workspace/Storlet_Grep/lib')
#put_storlet_dependency(url, token,'grep4j-with-dependencies.jar','/home/josep/Josep/workspace/Storlet_Grep/lib')
#put_storlet_dependency(url, token,'json-simple-1.1.1.jar','/home/josep/Josep/workspace/Storlet_Grep/lib')

#BLURFACES
#put_storlet_object(url, token,'blurfaces-1.0.jar','/home/josep/Josep/workspace/Storlet_BlurFaces/bin' ,'com.ibm.storlet.blurfaces.BlurFacesStorlet', 'commons-compress-1.2.jar,blur_faces_all.tar.gz')
#put_storlet_dependency(url, token,'commons-compress-1.2.jar','/home/josep/Josep/workspace/Storlet_BlurFaces/lib')
#put_storlet_dependency(url, token,'blur_faces_all.tar.gz','/home/josep/Josep/workspace/Storlet_BlurFaces/lib')

#WATERMARK
#put_storlet_object(url, token,'watermark-1.0.jar','/home/josep/Josep/workspace/Storlet_Watermark/bin' ,'it.rai.crit.activemediastore.storlets.WatermarkStorlet', 'commons-compress-1.2.jar,commons-io-1.3.2.jar,ffmpeg')
#put_storlet_dependency(url, token,'commons-compress-1.2.jar','/home/josep/Josep/workspace/Storlet_Watermark/lib')
#put_storlet_dependency(url, token,'commons-io-1.3.2.jar','/home/josep/Josep/workspace/Storlet_Watermark/lib')
#put_storlet_dependency(url, token,'ffmpeg','/home/josep/Josep/workspace/Storlet_Watermark/lib')

#TRANSCODER
#put_storlet_object(url, token,'transcoder-1.0.jar','/home/josep/Josep/workspace/Storlet_Transcoder/bin' ,'com.ibm.storlet.transcoder.TranscoderStorlet', 'commons-logging-1.1.3.jar,fontbox-1.8.4.jar,jempbox-1.8.4.jar,pdfbox-app-1.8.4.jar')
#put_storlet_dependency(url, token,'commons-logging-1.1.3.jar','/home/josep/Josep/workspace/Storlet_Transcoder/lib')
#put_storlet_dependency(url, token,'fontbox-1.8.4.jar','/home/josep/Josep/workspace/Storlet_Transcoder/lib')
#put_storlet_dependency(url, token,'jempbox-1.8.4.jar','/home/josep/Josep/workspace/Storlet_Transcoder/lib')
#put_storlet_dependency(url, token,'pdfbox-app-1.8.4.jar','/home/josep/Josep/workspace/Storlet_Transcoder/lib')
