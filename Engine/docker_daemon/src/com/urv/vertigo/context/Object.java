/*============================================================================
 18-Aug-2016    josep.sampe			Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.context;

import java.util.Map;
import org.slf4j.Logger;

import com.urv.vertigo.api.Swift;


public class Object {
	private String object;
	private Swift swift;
	private Logger logger_;	
	public Metadata metadata;
	public String timestamp;
	public String etag;
	public String lastModified;
	public String contentLength;
	public String backendTimestamp;
	public String contentType;
	
	public Object(Map<String, String> objectMetadata, String currentObject, Swift apiSwift, Logger logger) {
		object = currentObject;
		swift = apiSwift;
		logger_ = logger;
		//objectMetadata.entrySet().removeIf(entry -> entry.getKey().startsWith("X-Object-Sysmeta-Vertigo"));
		//objectMetadata.entrySet().removeIf(entry -> entry.getKey().startsWith("X-Object-Meta"));
		metadata = new Metadata();
		
		timestamp = objectMetadata.get("X-Timestamp");
		etag = objectMetadata.get("Etag");
		lastModified = objectMetadata.get("Last-Modified");
		contentLength = objectMetadata.get("Content-Length");
		backendTimestamp = objectMetadata.get("X-Backend-Timestamp");
		contentType = objectMetadata.get("Content-Type");
		
		// TODO: Put object metadata into cache

		logger_.trace("ApiObject created");
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

	public class Metadata { 
		 
		public String get(String key){
			return swift.metadata.get(object, "X-Object-Meta-"+key);
		}

		public void set(String key, String value){
			swift.metadata.set(object, key, value);
		}

		public Long incr(String key){
			Long newValue = swift.metadata.incr(object, key);
			return newValue;
		}
		
		public Long incrBy(String key, int value){
			Long newValue = swift.metadata.incrBy(object, key, value);
			return newValue;
		}
		
		public Long decr(String key){
			Long newValue = swift.metadata.decr(object, key);
			return newValue;
		}
		
		public Long decrBy(String key, Integer value){
			Long newValue = swift.metadata.decrBy(object, key, value);
			return newValue;
		}

		public void del(String key){
			swift.metadata.del(object, key);
		}
		
		public void flush(){
			swift.metadata.flush(object);
		}
		
	}
}