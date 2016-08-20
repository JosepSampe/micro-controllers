/*============================================================================
 21-Oct-2015    josep.sampe			Initial implementation.
 05-feb-2016	josep.sampe			Added Internal Client.
 17-Aug-2016	josep.sampe			Refactor 
 ===========================================================================*/
package com.urv.vertigo.daemon;

import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class ApiMicrocontroller {	
	public JSONObject metadata;

	public ApiMicrocontroller(Map<String, String> objectMetadata) {
		
		String mcMetadata = objectMetadata.get("X-Object-Sysmeta-Vertigo-Onget-Cbac-1.0.Jar");
		
		try{
			metadata = (JSONObject) new JSONParser().parse(mcMetadata);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		
	}
}