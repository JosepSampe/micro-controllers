/*============================================================================
 20-Aug-2016    josep.sampe			Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.api;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;
import redis.clients.jedis.Jedis;
import org.slf4j.Logger;


public class Redis {
	private Logger logger_;	
		
	private static String configFile = "/home/swift/docker_daemon.config";
	private Properties config;
	private String redisIp = "127.0.0.1";
	private int redisPort = 6379;
	private int redisDefaultDatabase = 5;
	private Jedis redis = null;

	
	public Redis(Logger logger){
		logger_ = logger;
		logger_.trace("Creating ApiRedis");

		try {
			logger_.trace("Loading configuration file "+configFile);
			config = new Properties();
			InputStream is = new FileInputStream(configFile);
			config.load(is);
			
			redisIp = config.getProperty("redis_ip");
			redisPort = Integer.parseInt(config.getProperty("redis_port"));
			redisDefaultDatabase = Integer.parseInt(config.getProperty("redis_db"));
		} catch (FileNotFoundException e) {
			logger_.trace("Config file not found"+configFile);
		} catch (IOException e) {
			logger_.trace("Failed to load config file");
		}

		redis = new Jedis(redisIp,redisPort);
		redis.select(redisDefaultDatabase+1);
		
		logger_.trace("ApiRedis created");
	}

	public Jedis getClient(){
		return redis;
	}	
}