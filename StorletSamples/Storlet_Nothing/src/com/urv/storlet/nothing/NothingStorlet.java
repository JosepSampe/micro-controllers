package com.urv.storlet.nothing;

import com.ibm.storlet.common.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import org.apache.commons.compress.utils.IOUtils;

/**
 * 
 * @author Josep Sampe
 *
 */

public class NothingStorlet implements IStorlet {
	/***
	 * Storlet invoke method. 
	 */
	@Override
	public void invoke(ArrayList<StorletInputStream> inStreams,
			ArrayList<StorletOutputStream> outStreams,
			Map<String, String> parameters,
			StorletLogger logger) throws StorletException {	

		logger.emitLog("Init Storlet");
        StorletInputStream storletInputStream = inStreams.get(0);
        InputStream inputStream = storletInputStream.getStream();
        HashMap<String, String> hashMap = storletInputStream.getMetadata();
        StorletObjectOutputStream storletObjectOutputStream = (StorletObjectOutputStream)outStreams.get(0);
        OutputStream outputStream = storletObjectOutputStream.getStream();
        storletObjectOutputStream.setMetadata((Map<String, String>)hashMap);
		
		try {  
			
            IOUtils.copy((InputStream)inputStream, (OutputStream)outputStream);
            inputStream.close();
            outputStream.close();
			
		} catch (UnsupportedEncodingException e) {
			logger.emitLog("Nothing Storlet - raised UnsupportedEncodingException: " + e.getMessage());
			throw new StorletException(e.getMessage());
		} catch (IOException e) {
			logger.emitLog("Nothing Storlet - raised IOException: " + e.getMessage());
			throw new StorletException(e.getMessage());
		}

	}
}