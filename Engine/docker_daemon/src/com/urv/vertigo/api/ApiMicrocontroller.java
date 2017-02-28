/*============================================================================
 18-Aug-2016    josep.sampe			Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.api;

import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;


public class ApiMicrocontroller {
	private String name;
	private String method;
	private Logger logger_;
	private String object;
	private ApiSwift swift;
	
	public JSONObject metadata = null;

	public ApiMicrocontroller(Map<String, String> objectMetadata, String mcName, String currentObject, 
							  String requestMethod, ApiSwift apiSwift, Logger logger) {
		name = mcName;
		method = requestMethod;
		logger_ = logger;
		object = currentObject;
		swift = apiSwift;
		
		String metadataKey = "x-object-sysmeta-vertigo-on"+method+"-"+name;
		for (String key: objectMetadata.keySet()){
			if (key.toLowerCase().equals(metadataKey)){
				String mcMetadata = objectMetadata.get(key);
				// TODO: check if mcMetadata is null
				try{
					metadata = (JSONObject) new JSONParser().parse(mcMetadata);
				} catch (ParseException e1) {
					e1.printStackTrace();
				}
			}
		}
		
		logger_.trace("ApiMicrocontroller created");
	}
	
	public void updateMetadata(){
		logger_.trace("Updating microcontroller metadata");
		swift.setMicrocontroller(object, name, method, metadata.toString());
	}
}