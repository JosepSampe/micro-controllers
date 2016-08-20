/*============================================================================
 18-Aug-2016    josep.sampe			Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.api;

import java.util.Map;

import org.slf4j.Logger;


public class ApiSwift {
	private String object;
	private ApiExternalBus apiBus;
	private Logger logger_;	
	private Map<String, String> metadata;
	public String timestamp;
	public String etag;
	public String lastModified;
	public String contentLength;
	public String backendTimestamp;
	public String contentType;
	
	public ApiSwift(Map<String, String> objectMetadata, String currentObject, ApiExternalBus externalApiBus, Logger logger) {
		object = currentObject;
		apiBus = externalApiBus;
		logger_ = logger;
		objectMetadata.entrySet().removeIf(entry -> entry.getKey().startsWith("X-Object-Sysmeta-Vertigo"));
		metadata = objectMetadata;
		timestamp = metadata.get("X-Timestamp");
		etag = metadata.get("Etag");
		lastModified = metadata.get("Last-Modified");
		contentLength = metadata.get("Content-Length");
		backendTimestamp = metadata.get("X-Backend-Timestamp");
		contentType = metadata.get("Content-Type");

		logger_.trace("ApiObject created");
	}
	
	public String getMetadata(String key){
		try{
			return metadata.get("X-Object-Meta-"+key);
		}catch (Exception e){
			return null;
		}
	}

	public void setMetadata(String key, String value){
		apiBus.setParam("source", object);
		apiBus.setParam("key", key);
		apiBus.setParam("value", value);
		apiBus.sendCommand("SWIFT","SET_METADATA");
	}
	
	
	public void copy(String dest){
		apiBus.setParam("source", object);
		apiBus.setParam("destination", dest);
		apiBus.sendCommand("SWIFT","COPY");
	}
	
	public void move(String dest){
		apiBus.setParam("source", object);
		apiBus.setParam("destination", dest);
		apiBus.sendCommand("SWIFT","MOVE");
	}
	
	public void delete(){
		apiBus.setParam("source", object);
		apiBus.sendCommand("SWIFT","DELETE");
	}
	
}