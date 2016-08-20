/*============================================================================
 21-Oct-2015    josep.sampe			Initial implementation.
 05-feb-2016	josep.sampe			Added Internal Client.
 17-Aug-2016	josep.sampe			Refactor 
 ===========================================================================*/
package com.urv.vertigo.daemon;

import java.util.Map;


public class ApiExternalBus {
	public Map<String, String> metadata;
	public String timestamp;
	public String etag;
	public String lastModified;
	public String contentLength;
	public String backendTimestamp;
	public String contentType;
	
	public ApiExternalBus(Map<String, String> objectMetadata) {
		objectMetadata.entrySet().removeIf(entry -> entry.getKey().startsWith("X-Object-Sysmeta-Vertigo"));
		metadata = objectMetadata;
		
		timestamp = metadata.get("X-Timestamp");
		etag = metadata.get("Etag");
		lastModified = metadata.get("Last-Modified");
		contentLength = metadata.get("Content-Length");
		backendTimestamp = metadata.get("X-Backend-Timestamp");
		contentType = metadata.get("Content-Type");

	}
}