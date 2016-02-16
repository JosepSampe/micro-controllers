/*============================================================================
08-Feb-2016    josep.sampe       Initial implementation.
 ===========================================================================*/
package com.urv.handler.webprefetching;

import java.util.Map;
import com.urv.controller.daemon.*;

public class PrefetchingHandler implements IHandler {
	/***
	 * Handler invoke method. 
	 */

	@Override
	public void invoke( HandlerLogger logger, HandlerMetadata meta, Map<String, String> file_md, 
						Map<String, String> req_md,  HandlerOutput out) {
		
		out.execStorlets();
		
		logger.emitLog("*********** Init Web Prefetching Handler ************");
	
        String prefetch_list = file_md.get("prefetch_list").toString();	        

		out.setExecParam("source_file_list", prefetch_list);
		out.setExecParam("object_path", file_md.get("X-Object-Meta-Path"));
		out.execCommand("PREFETCH",req_md);

		logger.emitLog("************ End Web Prefetching Handler ************");

	}
}