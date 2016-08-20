/*============================================================================
 18-Aug-2016    josep.sampe		Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.api;

import java.io.FileDescriptor;
import org.slf4j.Logger;
import java.util.Map;


public class Api {
	
	private Logger logger_;	
	public ApiLogger logger;
	public ApiStorlet storlet;
	public ApiRequest request;
	public ApiObject object;
	public ApiSwift swift;
	public ApiMicrocontroller microcontroller;


	public Api(String mcName, FileDescriptor log, FileDescriptor toSwift, Map<String, String> objectMd, 
			   Map<String, String> reqMd, Logger localLog) 
	{
		String tenantId = reqMd.get("X-Tenant-Id");
		String currentObject = reqMd.get("Referer").split("/",6)[5];
		String token = reqMd.get("X-Auth-Token");
		String method = reqMd.get("X-Method");

		logger_ = localLog;
		swift = new ApiSwift(token, tenantId, logger_);
		logger = new ApiLogger(log, logger_);
		microcontroller = new ApiMicrocontroller(objectMd, mcName, currentObject, method, swift, logger_);
		request = new ApiRequest(toSwift, reqMd, logger_);
		storlet = new ApiStorlet(toSwift, logger_);
		object = new ApiObject(objectMd, currentObject, swift, logger_);
		
		logger_.trace("Full API created");
	}
	
}