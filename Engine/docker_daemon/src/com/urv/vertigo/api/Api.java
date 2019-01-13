/*============================================================================
 18-Aug-2016    josep.sampe		Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.api;

import java.io.FileDescriptor;
import org.slf4j.Logger;
import java.util.Map;


public class Api {
	
	private Logger logger_;	
	public Storlet storlet;
	public Swift swift;


	public Api(FileDescriptor toSwift, Map<String, String> reqMd, Logger localLog) 
	{	
		logger_ = localLog;
		logger_.trace("- Creating API module");
		
		String tenantId = reqMd.get("X-Tenant-Id");
		String token = reqMd.get("X-Auth-Token");

		swift = new Swift(token, tenantId, logger_);
		storlet = new Storlet(toSwift, logger_);
		
		logger_.trace("- Full API created");
	}
	
}