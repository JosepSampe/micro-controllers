/*============================================================================
 18-Aug-2016    josep.sampe		Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.context;

import java.io.FileDescriptor;
import org.slf4j.Logger;
import com.urv.vertigo.api.Swift;
import java.util.Map;


public class Context {
	
	private Logger logger_;	
	public Log logger;
	public Request request;
	public SwiftObject object;
	public Microcontroller microcontroller;


	public Context(Swift swift, String mcName, FileDescriptor log, FileDescriptor toSwift, Map<String, String> objectMd, 
			   	   Map<String, String> reqMd, Logger localLog) 
	{	
		logger_ = localLog;
		logger_.trace("# Creating Context module");
		
		String currentObject = reqMd.get("Referer").split("/",6)[5];
		String method = reqMd.get("X-Method");

		logger = new Log(log, logger_);
		microcontroller = new Microcontroller(objectMd, mcName, currentObject, method, swift, logger_);
		request = new Request(toSwift, reqMd, logger_);
		object = new SwiftObject(objectMd, currentObject, swift, logger_);
		
		logger_.trace("# Full Context created");
	}
	
}