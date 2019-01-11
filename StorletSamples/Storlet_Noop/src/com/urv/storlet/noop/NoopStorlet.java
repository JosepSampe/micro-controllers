package com.urv.storlet.noop;

import com.ibm.storlet.common.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 
 * @author Josep Sampe
 *
 */

public class NoopStorlet implements IStorlet {
	
	/***
	 * Storlet invoke method
	 */
	@Override
	public void invoke(ArrayList<StorletInputStream> inStreams,
			ArrayList<StorletOutputStream> outStreams,
			Map<String, String> parameters,
			StorletLogger logger) throws StorletException {

		StorletInputStream sis = inStreams.get(0);
		InputStream is = sis.getStream();
		HashMap<String, String> metadata = sis.getMetadata();

		StorletObjectOutputStream sos = (StorletObjectOutputStream)outStreams.get(0);
		OutputStream os = sos.getStream();
		sos.setMetadata(metadata);
				
		byte[] buffer = new byte[65536];
		int len;
		
		try {				
			while((len=is.read(buffer)) != -1) {
				os.write(buffer, 0, len);
			}
			is.close();
			os.close();
		} catch (IOException e) {
			logger.emitLog("NOP Storlet - raised IOException: " + e.getMessage());
		}
	}
}