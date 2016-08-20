/*============================================================================
 20-Aug-2016    josep.sampe			Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.api;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import redis.clients.jedis.Jedis;
import org.slf4j.Logger;


public class ApiSwift {
	private Logger logger_;	
	private String token;
	private String storageUrl;
	private String tenantId;
	private List<String> unnecessaryHeaders = Arrays.asList(null, "Connection", "X-Trans-Id", "Date");
	
	private String swiftBackend = "http://192.168.2.1:8080/v1/"; // TODO: get from cofig file
	private String redisHost = "192.168.2.1"; // TODO: get from cofig file
	private int redisPort = 6379; // TODO: get from cofig file
	private int defaultDatabase = 5; // TODO: get from cofig file
	private Jedis redis = new Jedis(redisHost,redisPort);

	
	public ApiSwift(String strToken, String projectId, Logger logger) {
		token = strToken;
		tenantId = projectId;
		storageUrl = swiftBackend+"AUTH_"+projectId+"/";
		logger_ = logger;
		logger_.trace("ApiSwift created");
		redis.select(defaultDatabase);		
	}
	
	private Map<String, String> getAllMetadata(String source){
		Map<String, String> metadata = new HashMap<String, String>();
		String redisPrefix = tenantId+"/"+source+"_";
		Set<String> keys = redis.keys(redisPrefix+"*");

		if (keys.size() == 0){
			HttpURLConnection conn = newConnection(source);
			try {
				conn.setRequestMethod("HEAD");
			} catch (ProtocolException e) {
				logger_.trace("Error: Bad Protocol");
			}
			sendRequest(conn);
			Map<String, List<String>> headers = conn.getHeaderFields();		

			for (Entry<String, List<String>> entry : headers.entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue().get(0);
				if (!unnecessaryHeaders.contains(key) && !key.startsWith("Vertigo")){
					redis.set(redisPrefix+key.toLowerCase(), value);
					metadata.put(key.toLowerCase(), value);
				}
			}
		} else {
			for (String key: keys){
				String value = redis.get(key);
				metadata.put(key.replace(redisPrefix, ""), value);
			}
		}
		
		return metadata;
	}	

	public String getMetadata(String source, String key){
		String redisPrefix = tenantId+"/"+source+"_";
		String value = redis.get(redisPrefix+key.toLowerCase());
		if (value == null){
			Map<String, String> metadata = getAllMetadata(source);
			value = metadata.get(key.toLowerCase());
		}
		return value;
	}
	
	public void setMetadata(String source, String key, String value){
		String redisPrefix = tenantId+"/"+source+"_";
		redis.set(redisPrefix+"x-object-meta-"+key.toLowerCase(),value);
		flushMetadata(source);	
	}
	
	public void setMicrocontroller(String source, String mc, String method, String metadata){
		HttpURLConnection conn = newConnection(source);
		conn.setRequestProperty("X-Vertigo-on"+method, mc);
		try {
			conn.setRequestMethod("POST");
		} catch (ProtocolException e) {
			logger_.trace("Error: Bad Protocol");
		}
		sendMcMetadata(conn, metadata);
	}
	
	public void copy(String source, String dest){

	}
	
	public void move(String source, String dest){

	}
	
	public void delete(String source){
		HttpURLConnection conn = newConnection(source);
		try {
			conn.setRequestMethod("DELETE");
		} catch (ProtocolException e) {
			logger_.trace("Error: Bad Protocol");
		}
		sendRequest(conn);
	}
	
	public void flushMetadata(String source){
		String redisPrefix = tenantId+"/"+source+"_";
		Set<String> keys = redis.keys(redisPrefix+"x-object-meta-*");
		if (keys.size()>0){
			HttpURLConnection conn = newConnection(source);
			for (String redisKey : keys) {
				String redisValue = redis.get(redisKey);
				conn.setRequestProperty(redisKey.replace(redisPrefix, ""), redisValue);	
			}
			try {
				conn.setRequestMethod("POST");
			} catch (ProtocolException e) {
				logger_.trace("Error: Bad Protocol");
			}
			sendRequest(conn);
		}
	}
	
	private HttpURLConnection newConnection(String source){
		String storageUri = storageUrl+source;
		URL url = null;
		HttpURLConnection conn = null;
		try {
			url = new URL(storageUri);
			conn = (HttpURLConnection) url.openConnection();
		} catch (MalformedURLException e) {
			logger_.trace("Error: Malformated URL");
		} catch (IOException e) {
			logger_.trace("Error opeing connection");
		}
		conn.setRequestProperty("X-Auth-Token", token);
		return conn;	
	}
	
	private int sendMcMetadata(HttpURLConnection conn, String metadata){
		conn.setDoOutput(true);
		OutputStream os;
		int status = 404;
		try {
			os = conn.getOutputStream();
			os.write(metadata.getBytes());
			os.close();
			status = conn.getResponseCode();
		} catch (IOException e) {
			logger_.trace("Error setting microcontroller metadata");
		}
		conn.disconnect();	
		return status;
	  }
	
	private int sendRequest(HttpURLConnection conn){
		int status = 404;
		try {
			status = conn.getResponseCode();
		} catch (IOException e) {
			logger_.trace("Error getting response");
		}
		conn.disconnect();
		return status;
	  }
	
}