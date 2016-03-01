/*============================================================================
08-Feb-2016    josep.sampe       Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.mc.prefetching;

import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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
		
        
		String metastorlet = meta.getMetadata();        
		JSONObject jsonMetadata = null;
		
		try {
			jsonMetadata = (JSONObject)new JSONParser().parse(metastorlet);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}

		logger.emitLog(req_md.get("Referer").toString());
		
		
		out.setExecParam("source_file_list", jsonMetadata.get("prefetch_list").toString());
		out.setExecParam("object_path", req_md.get("Referer").toString());
		out.execCommand("PREFETCH",req_md);

		logger.emitLog("************ End Web Prefetching MicroController ************");
	}
}