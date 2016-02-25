/*============================================================================
08-Feb-2016    josep.sampe       Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.mc.prefetching;

import java.util.Map;
import com.urv.vertigo.daemon.*;

public class PrefetchingHandler implements IHandler {
	/***
	 * MicroController invoke method. 
	 */

	@Override
	public void invoke( HandlerLogger logger, HandlerMetadata meta, Map<String, String> file_md, 
						Map<String, String> req_md,  HandlerOutput out) {
		logger.emitLog("*********** Init Web Prefetching MicroController ************");
		
		out.execStorlets(); // No Storlets to execute
        String prefetch_list = file_md.get("prefetch_list").toString();	        

		out.setExecParam("source_file_list", prefetch_list);
		out.setExecParam("object_path", file_md.get("X-Object-Meta-Path"));
		out.execCommand("PREFETCH",req_md);

		logger.emitLog("************ End Web Prefetching MicroController ************");
	}
}