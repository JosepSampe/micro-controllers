/*============================================================================
 18-Aug-2016    josep.sampe		Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.api;

import java.io.FileDescriptor;
import org.slf4j.Logger;
import redis.clients.jedis.Jedis;
import java.util.Map;


public class Api {
	
	private Logger logger_;	
	public Storlet storlet;
	public Swift swift;
	public Jedis redis;


	public Api(FileDescriptor toSwift, Map<String, String> reqMd, Logger localLog) 
	{	
		logger_ = localLog;
		logger_.trace("# Creating API module");
		
		String tenantId = reqMd.get("X-Tenant-Id");
		String token = reqMd.get("X-Auth-Token");

		swift = new Swift(token, tenantId, logger_);
		storlet = new Storlet(toSwift, logger_);
		redis = new Redis(logger_).getClient();
		
		logger_.trace("# Full API created");
	}
	
}