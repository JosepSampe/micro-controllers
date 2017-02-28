/*============================================================================
 20-Aug-2016    josep.sampe			Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import redis.clients.jedis.Jedis;
import net.spy.memcached.MemcachedClient;
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
	private int redisDefaultDatabase = 5; // TODO: get from cofig file
	private String memcachedHost = "192.168.2.1"; // TODO: get from cofig file
	private int memcachedPort = 11211; // TODO: get from cofig file
	
	private Jedis redis = null;
	private MemcachedClient mc = null;
	public Metadata metadata;


	public ApiSwift(String strToken, String projectId, Logger logger) {
		token = strToken;
		tenantId = projectId;
		storageUrl = swiftBackend+"AUTH_"+projectId+"/";
		logger_ = logger;

		redis = new Jedis(redisHost,redisPort);
		redis.select(redisDefaultDatabase);
		metadata = new Metadata();

		try {
			mc = new MemcachedClient(new InetSocketAddress(memcachedHost, memcachedPort));
		} catch (IOException e) {
			logger_.trace("Failed to create Memcached client");
		}
		
		logger_.trace("ApiSwift created");
	}

	public class Metadata { 
		
		private Map<String, String> getAll(String source){
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
		 
		public String get(String source, String key){
			String redisPrefix = tenantId+"/"+source+"_";
			String value = redis.get(redisPrefix+key.toLowerCase());
			if (value == null){
				Map<String, String> metadata = getAll(source);
				value = metadata.get(key.toLowerCase());
			}
			return value;
		}
				
		public void set(String source, String key, String value){
			String redisPrefix = tenantId+"/"+source+"_";
			String redisKey = redisPrefix+"x-object-meta-"+key.toLowerCase();
			redis.set(redisKey,value);
			flush(source);
		}
		
		public Long incr(String source, String key){
			logger_.trace("ApiSwift: incrementing value of "+key);
			String redisPrefix = tenantId+"/"+source+"_";
			String redisKey = redisPrefix+"x-object-meta-"+key.toLowerCase();
			Long newValue = redis.incr(redisKey);
			flush(source);
			return newValue;
		}
		
		public Long incrBy(String source, String key, int value){
			logger_.trace("ApiSwift: incrementing value of "+key);
			String redisPrefix = tenantId+"/"+source+"_";
			String redisKey = redisPrefix+"x-object-meta-"+key.toLowerCase();
			Long newValue = redis.incrBy(redisKey, value);
			flush(source);
			return newValue;
		}
		
		public Long decr(String source, String key){
			logger_.trace("ApiSwift: incrementing value of "+key);
			String redisPrefix = tenantId+"/"+source+"_";
			String redisKey = redisPrefix+"x-object-meta-"+key.toLowerCase();
			Long newValue = redis.decr(redisKey);
			flush(source);
			return newValue;
		}
		
		public Long decrBy(String source, String key, int value){
			String redisPrefix = tenantId+"/"+source+"_";
			String redisKey = redisPrefix+"x-object-meta-"+key.toLowerCase();
			Long newValue = redis.decrBy(redisKey, value);
			flush(source);
			return newValue;
		}

		public void del(String source, String key){
			String redisPrefix = tenantId+"/"+source+"_";
			String redisKey = redisPrefix+"x-object-meta-"+key.toLowerCase();
			redis.del(redisKey);
			flush(source);
		}
		
		public void flush(String source){
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
		
	}

	/*
	public BufferedReader get(String source){
		BufferedReader reader = null;
		HttpURLConnection conn = newConnection(source);
		conn.setDoOutput(false);
		conn.setDoInput(true);

		String tok = UUID.randomUUID().toString();
		String key = "VERTIGO_TOKEN_"+tok.split("-")[0]+"_AUTH_"+tenantId+"/"+source;
		mc.set(MD5(key), 10, tok);
		
		try {
			conn.setRequestProperty("X-Vertigo-Token", tok);
			conn.setRequestMethod("GET");
			reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		} catch (ProtocolException pe) {
			logger_.trace("Error: Bad Protocol");
		} catch (IOException ioe) {
			logger_.trace("Error: Can't get Input Stream");
		}
		return reader;
	}
	*/

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
		if (!source.equals(dest)){
			HttpURLConnection conn = newConnection(dest);
			conn.setFixedLengthStreamingMode(0);
			conn.setRequestProperty("X-Copy-From", source);
			try {
				conn.setRequestMethod("PUT");
			} catch (ProtocolException e) {
				logger_.trace("Error: Bad Protocol");
			}
			sendRequest(conn);
			logger_.trace("Copying "+source+" object to "+dest);
		}
	}
	
	public void move(String source, String dest){
		if (!source.equals(dest)){
			HttpURLConnection conn = newConnection(source);
			conn.setFixedLengthStreamingMode(0);
			conn.setRequestProperty("X-Vertigo-Link-To", dest);
			try {
				conn.setRequestMethod("PUT");
			} catch (ProtocolException e) {
				logger_.trace("Error: Bad Protocol");
			}
			sendRequest(conn);
			logger_.trace("Moving "+source+" object to "+dest);
		}
	}
	
	public void prefetch(String source){
		String id =  "AUTH_"+tenantId+"/"+source;
		logger_.trace("Prefetching "+id);
		String hash = MD5(id);
		String data = "";
		mc.set(hash, 600, data);
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
		
	private HttpURLConnection newConnection(String source){
		String storageUri = storageUrl+source;
		URL url = null;
		HttpURLConnection conn = null;
		try {
			url = new URL(storageUri);
			conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestProperty("X-Auth-Token", token);
			conn.setRequestProperty("User-Agent", "vertigo/microcontroller");
		} catch (MalformedURLException e) {
			logger_.trace("Error: Malformated URL");
		} catch (IOException e) {
			logger_.trace("Error opeing connection");
		}
		return conn;	
	}
	
	private int sendMcMetadata(HttpURLConnection conn, String metadata){
		OutputStream os;
		int status = 404;
		try {
			conn.connect();
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
			conn.connect();
			status = conn.getResponseCode();
		} catch (IOException e) {
			logger_.trace("Error getting response");
		}
		conn.disconnect();
		return status;
	}
	
	private String MD5(String key) {
        MessageDigest m = null;
		try {
			m = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			logger_.trace("Hash Algorith error");
		}
        m.update(key.getBytes(),0,key.length());
        String hash = new BigInteger(1,m.digest()).toString(16);
        while (hash.length() < 32)
        	hash = "0"+hash;
        return hash;
	}
	
}