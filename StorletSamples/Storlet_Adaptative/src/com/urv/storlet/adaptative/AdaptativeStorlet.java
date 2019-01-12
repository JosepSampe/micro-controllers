package com.urv.storlet.adaptative;

import org.openstack.storlet.common.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

/**
 * 
 * @author Josep Sampé
 *
 */

public class AdaptativeStorlet implements IStorlet {
	
	/***
	 * Storlet invoke method
	 */
	@Override
	public void invoke(ArrayList<StorletInputStream> inStreams,
			ArrayList<StorletOutputStream> outStreams,
			Map<String, String> parameters,
			StorletLogger logger) throws StorletException {
		
		logger.emitLog("Init Storlet");
		StorletInputStream sis = inStreams.get(0);
		InputStream is = sis.getStream();
		HashMap<String, String> metadata = sis.getMetadata();
		
		String p_r = parameters.get("p");
		
		if (p_r == null) {
			p_r = "1";
		}
		metadata.put("parallel_requests", p_r);

		int parallel_requests = Integer.parseInt(p_r);

		
		StorletObjectOutputStream sos = (StorletObjectOutputStream)outStreams.get(0);
		OutputStream os = sos.getStream();
		sos.setMetadata(metadata);
		
		int bw_limit,throughput,sleep;
		
		if (parallel_requests==1){
			throughput=65535;
			sleep=0;
		}else{
			sleep=1;
			bw_limit=110/parallel_requests;
			throughput=(bw_limit*1024*1024)/(770-bw_limit);
		}

		logger.emitLog("Throughput: " + throughput);
		
		try {  
			
			byte[] buffer = new byte[throughput];
	    	int len;
	    	while ((len = is.read(buffer)) != -1) {
	        	Thread.sleep(sleep);
	    		os.write(buffer, 0, len);
	    	}
			is.close();
			os.close();
			
		} catch (UnsupportedEncodingException e) {
			logger.emitLog("Nothing Storlet - raised UnsupportedEncodingException: " + e.getMessage());
			throw new StorletException(e.getMessage());
		} catch (IOException e) {
			logger.emitLog("Nothing Storlet - raised IOException: " + e.getMessage());
			throw new StorletException(e.getMessage());
		} catch(InterruptedException ex) {
    	    Thread.currentThread().interrupt();
    	}

	
	}
}