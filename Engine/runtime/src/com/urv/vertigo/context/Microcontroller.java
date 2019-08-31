/*============================================================================
 18-Aug-2016    josep.sampe			Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.context;

import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import com.urv.vertigo.api.Swift;


public class Microcontroller {
	private String name;
	private String method;
	private Logger logger_;
	private String object;
	private Swift swift;
	
	private JSONObject jsonparameters = null;
	public Map<String, String> parameters = new HashMap<String, String>();

	public Microcontroller(Map<String, String> objectMetadata, String mcName, String currentObject, 
							  String requestMethod, Swift apiSwift, Logger logger) {
		name = mcName;
		method = requestMethod;
		logger_ = logger;
		object = currentObject;
		swift = apiSwift;
		
		String metadataKey = "x-object-sysmeta-vertigo-on"+method+"-"+name;
		for (String key: objectMetadata.keySet()){
			if (key.toLowerCase().equals(metadataKey)){
				logger_.trace("Parsing micro-controller parameters in: "+metadataKey);
				String mcMetadata = objectMetadata.get(key);
				logger_.trace("Parameters: "+mcMetadata);
				// TODO: check if mcMetadata is null
				try{
					jsonparameters = (JSONObject) new JSONParser().parse(mcMetadata);

					for (Object pkey : jsonparameters.keySet()) {
				        String keyStr = (String) pkey;
				        String keyvalue = (String) jsonparameters.get(keyStr);
				        parameters.put(keyStr, keyvalue);
				    }
					
				} catch (ParseException e1) {
					logger_.error("Failed parsing micro-controller parameters");
				}
			}
		}
		
		logger_.trace("Context Micro-controller created");
	}
	
	public void updateParameters(){
		logger_.trace("Updating micro-controller metadata");
		swift.setMicrocontroller(object, name, method, parameters.toString());
	}
}