/*============================================================================
 20-Oct-2015    josep.sampe       Initial implementation.
 ===========================================================================*/
package com.urv.handler.cbac;

import java.util.Map;

import com.urv.controller.daemon.*;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class CBACHandler implements IHandler {
	/***
	 * Handler invoke method. 
	 */
	
	
	@Override
	public void invoke( HandlerLogger logger, HandlerMetadata meta, Map<String, String> file_md, 
						Map<String, String> req_md,  HandlerOutput out) {

		logger.emitLog("*********** Init CBAC Handler ************");

        String metastorlet = meta.getMetadata();

        JSONObject jsonMetadata = null;
        
		try {
			jsonMetadata = (JSONObject)new JSONParser().parse(metastorlet);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		
		String user_roles = req_md.get("X-Roles");
        String role = jsonMetadata.get("role").toString();
        String allowed_cols = jsonMetadata.get("allowed_cols").toString();
        
        logger.emitLog("User roles: "+user_roles);
        logger.emitLog("Role: "+role+", Allowed columns: "+allowed_cols);       


        // STORLET EXECUTION
		out.setStorlet(0,"adult-1.0.jar","select="+allowed_cols,"object-server");
		out.execStorlets();

		logger.emitLog("************ End CBAC Handler ************");

	}
}