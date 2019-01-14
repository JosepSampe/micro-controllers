/*============================================================================
 18-Aug-2016    josep.sampe			Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.context;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import org.json.simple.JSONObject;
import org.slf4j.Logger;


public class Request {
	private FileOutputStream stream;
	private JSONObject outMetadata = new JSONObject();
	private Logger logger_;
	private Map<String, String> metadata;
	public String method;
	public String currentServer;
	public String timestamp;
	public String projectDomainId;
	public String userDomainName;
	public String userAgent;
	public String acceptEncoding;
	public String user;
	public String transId;
	public String projectId ;
	public String roles;
	public String userDomainId;
	public String tenantName;
	public String contentType;
	public String role;
	public String tenantId;
	public String backendStoragepolicyIndex;
	public String accept;
	public String userId;
	public String tenant;
	public String referer;
	public String connection;
	public String host;
	public String projectName;
	public String projectDomainName;
	public String identityStatus;
	public String authToken;
	public String isAdminProject;
	public String userName;

	public Request(FileDescriptor fd, Map<String, String> requestMetadata, Logger logger) {
		stream = new FileOutputStream(fd);
		metadata = requestMetadata;
		logger_ = logger;
		this.mapKeys();
		
		logger_.trace("Context Request created");
	}

	private void mapKeys(){
		method = metadata.get("X-Method");
		currentServer = metadata.get("X-Current-Server");
		timestamp = metadata.get("X-Timestamp");
		projectDomainId = metadata.get("X-Project-Domain-Id");
		userDomainName = metadata.get("X-User-Domain-Name");
		userAgent = metadata.get("User-Agent");
		acceptEncoding = metadata.get("Accept-Encoding");
		user = metadata.get("X-User");
		transId = metadata.get("X-Trans-Id");
		projectId = metadata.get("X-Project-Id");
		roles = metadata.get("X-Roles");
		userDomainId = metadata.get("X-User-Domain-Id");
		tenantName = metadata.get("X-Tenant-Name");
		contentType = metadata.get("Content-Type");
		role = metadata.get("X-Role");
		tenantId = metadata.get("X-Tenant-Id");
		backendStoragepolicyIndex = metadata.get("X-Backend-Storage-Policy-Index");
		accept = metadata.get("Accept");
		userId = metadata.get("X-User-Id");
		tenant = metadata.get("X-Tenant");
		referer = metadata.get("Referer");
		connection = metadata.get("Connection");
		host = metadata.get("Host");
		projectName = metadata.get("X-Project-Name");
		projectDomainName = metadata.get("X-Project-Domain-Name");
		identityStatus = metadata.get("X-Identity-Status");
		authToken = metadata.get("X-Auth-Token");
		isAdminProject = metadata.get("X-Is-Admin-Project");
		userName = metadata.get("X-User-Name");
	}
	
	@SuppressWarnings("unchecked")
	public void forward(){	
		outMetadata.put("command","CONTINUE");
		this.execute();
	}
	
	@SuppressWarnings("unchecked")
	public void cancel(String message){	
		outMetadata.put("command", "CANCEL");
		outMetadata.put("message", message);
		this.execute();
	}
	
	@SuppressWarnings("unchecked")
	public void rewire(String object_id){	
		outMetadata.put("command", "REWIRE");
		outMetadata.put("object_id", object_id);
		this.execute();
	}
	
	private void execute() {
		try {
			stream.write(outMetadata.toString().getBytes());
			this.flush();
		} catch (IOException e) {
			logger_.trace("Error sending command on Context Request");
		}
	}
	
	private void flush() {
		try {
			stream.flush();
		} catch (IOException e) {
			logger_.trace("Error flushing command on Context Request");
		}
	}
}