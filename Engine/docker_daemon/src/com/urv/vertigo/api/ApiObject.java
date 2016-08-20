/*============================================================================
 18-Aug-2016    josep.sampe			Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.api;

import java.util.Map;
import org.slf4j.Logger;


public class ApiObject {
	private String object;
	private ApiSwift swift;
	private Logger logger_;	
	private Map<String, String> metadata;
	public String timestamp;
	public String etag;
	public String lastModified;
	public String contentLength;
	public String backendTimestamp;
	public String contentType;
	
	public ApiObject(Map<String, String> objectMetadata, String currentObject, ApiSwift apiSwift, Logger logger) {
		object = currentObject;
		swift = apiSwift;
		logger_ = logger;
		loadMetadataToRedis(objectMetadata);
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
		return swift.getMetadata(object, "X-Object-Meta-"+key);
	}

	public void setMetadata(String key, String value){
		swift.setMetadata(object, key, value);
	}
	
	public void copy(String dest){
		swift.copy(object, dest);
	}
	
	public void move(String dest){
		swift.move(object, dest);
	}
	
	public void delete(){
		swift.delete(object);
	}
	
	public void flushMetadata(){
		swift.flushMetadata(object);
	}
	
	private void loadMetadataToRedis(Map<String, String> objectMetadata){
		objectMetadata.entrySet().removeIf(entry -> entry.getKey().startsWith("X-Object-Sysmeta-Vertigo"));
		objectMetadata.entrySet().removeIf(entry -> entry.getKey().startsWith("X-Object-Meta"));
	}
	
}