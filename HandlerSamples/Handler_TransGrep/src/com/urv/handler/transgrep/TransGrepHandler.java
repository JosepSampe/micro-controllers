/*============================================================================
 27-Jan-2016    josep.sampe       Initial implementation.
 ===========================================================================*/
package com.urv.handler.transgrep;

import java.util.Map;

import com.urv.controller.daemon.*;

public class TransGrepHandler implements IHandler {
	/***
	 * Handler invoke method. 
	 */
	
	@Override
	public void invoke( HandlerLogger logger, HandlerMetadata meta, Map<String, String> file_md, Map<String, String> req_md,  HandlerOutput out) {
		
    	// CALCULATE TIME ------------
		long startTime, estimatedTime;
		startTime = System.nanoTime();
    	// ---------------------------
		logger.emitLog("*********** Init TransGrep Handler ************");

        // STORLET EXECUTION
		out.setStorlet(0,"transcoder-1.0.jar","","object-server");
		out.setStorlet(1,"grep-1.0.jar","regexp=*^a*","object-server");
		out.execStorlets();
		
		logger.emitLog("To execute:" + out.getStorletList());
				
        // CALCULATE TIME ----------
        estimatedTime = System.nanoTime() - startTime;			
        logger.emitLog("Total Execution Time : " + estimatedTime);
		//--------------------------
		logger.emitLog("************ End TransGrep Handler ************");

	}
}