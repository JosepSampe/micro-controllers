/*============================================================================
 20-Oct-2015    josep.sampe       Initial implementation.
 ===========================================================================*/
package com.urv.handler.counter;

import java.util.Map;

import com.urv.controller.daemon.*;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class CounterHandler implements IHandler {
	/***
	 * Handler invoke method. 
	 */
	
	
	@SuppressWarnings("unchecked")
	@Override
	public void invoke( HandlerLogger logger, HandlerMetadata meta, Map<String, String> file_md, 
						Map<String, String> req_md,  HandlerOutput out) {
		
		logger.emitLog("*********** Init Counter Handler ************");
    	// CALCULATE TIME ------------
		long startTime, estimatedTime;
		startTime = System.nanoTime();
    	// ---------------------------
		
		// ASYNC EXECUTION; When we send a message through "out", the framework continue to work, 
		// and the rest of code in this micro-controller is executed asynchronously.
		out.execStorlets();	
		
        String metastorlet = meta.getMetadata();        
        JSONObject jsonMetadata = null;
        
		try {
			jsonMetadata = (JSONObject)new JSONParser().parse(metastorlet);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
        
        java.util.Date date= new java.util.Date();
        int accessed = Integer.parseInt(jsonMetadata.get("accessed").toString()) + 1;
        
        
        jsonMetadata.put("accessed",accessed);
        jsonMetadata.put("last_access",date.getTime());
        
        logger.emitLog("---------- NEW INFORMATION ----------");
        logger.emitLog("Accessed: " + jsonMetadata.get("accessed"));
        logger.emitLog("Last access: " + jsonMetadata.get("last_access"));
        logger.emitLog("-------------------------------------");
        
        
        /*
		for (Map.Entry<String, String> entry : file_md.entrySet())
			logger.emitLog(entry.getKey() + " : " + entry.getValue());
		for (Map.Entry<String, String> entry : req_md.entrySet())
			logger.emitLog(entry.getKey() + " : " + entry.getValue());
        */
      
        if (req_md.get("X-Copy-File") == null){
        	meta.updateMetadata(jsonMetadata.toString());
        	
			if (accessed==10){
				logger.emitLog("10 accesses. Going to delete the object.");
				out.setExecParam("source_file", req_md.get("Referer").split(" ")[1]);
				out.execCommand("DELETE",req_md);
			}
			
			/*
			out.setExecParam("source_file", req_md.get("Referer").split(" ")[1]);
			out.setExecParam("source_type", "processed");
			out.setExecParam("dest_file", "data/sample_3mb.gz");	
			out.execCommand("COPY",req_md);

			out.setExecParam("source_file_list", req_md.get("Referer").split(" ")[1]);
			out.execCommand("PREFETCH",req_md);
			*/
		}
        
        		
        logger.emitLog("To execute:" + out.getStorletList());
		
        // CALCULATE TIME ----------
        estimatedTime = System.nanoTime() - startTime;			
        logger.emitLog("Total Execution Time : " + estimatedTime);
        // ---------------------------
		logger.emitLog("************ End Counter Handler ************");

	}
}