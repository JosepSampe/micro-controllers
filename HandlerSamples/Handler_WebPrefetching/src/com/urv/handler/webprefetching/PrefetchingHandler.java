/*============================================================================
08-Feb-2016    josep.sampe       Initial implementation.
 ===========================================================================*/
package com.urv.handler.webprefetching;

import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.urv.controller.daemon.*;

public class PrefetchingHandler implements IHandler {
	/***
	 * Handler invoke method. 
	 */
	

	@Override
	public void invoke( HandlerLogger logger, HandlerMetadata meta, Map<String, String> file_md, 
						Map<String, String> req_md,  HandlerOutput out) {
		
		out.execStorlets();
		
    	// CALCULATE TIME ------------
		long startTime, estimatedTime;
		startTime = System.nanoTime();
    	// ---------------------------
		logger.emitLog("*********** Init Web Prefetching Handler ************");

		String metastorlet = meta.getMetadata();
        JSONObject jsonMetadata = null;

		try {
			jsonMetadata = (JSONObject)new JSONParser().parse(metastorlet);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
        String prefetch_list = jsonMetadata.get("prefetch_tag").toString();	        

		out.setExecParam("source_file_list", prefetch_list);
		out.setExecParam("object_path", file_md.get("X-Object-Meta-Path"));
		//out.execCommand("PREFETCH",req_md);

		
        // CALCULATE TIME ----------
        estimatedTime = System.nanoTime() - startTime;			
        logger.emitLog("Total Execution Time : " + estimatedTime);
		//--------------------------
		logger.emitLog("************ End Web Prefetching Handler ************");

	}
}