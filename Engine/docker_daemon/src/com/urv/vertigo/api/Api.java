/*============================================================================
 19-Oct-2015    josep.sampe		Initial implementation.
 03-Feb-2016	josep.sampe		Added bus to Internal Client
 17-Aug-2016	josep.sampe		Refactor
 ===========================================================================*/
package com.urv.vertigo.daemon;

import java.io.FileDescriptor;

import org.slf4j.Logger;

import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import com.ibm.storlet.sbus.SBus;

public class Api {

	private Logger logger_;
	private MicrocontrollerLogger logFile_;
	private MicrocontrollerOutput out_pipe_;
	private String strName_;
	private String strMainClass_;
	private String strDependencies_;
	private IMicrocontroller microcontroller_;
	private Map<String, String> object_md_;
	private Map<String, String> req_md_;


	private IMicrocontroller loadMicrocontroller(final String strHandlerClassName) {
		IMicrocontroller microcontroller = null;
		List<String> dependencies = Arrays.asList(strDependencies_.split(","));
		URL[] searchPath = new URL[dependencies.size()+1];
		Integer index = 0;	

		try {	
			//logger_.info("/home/swift/"+strHandlerClassName_+"/"+strHandlerName_);
			
			searchPath[index++] = new File("/home/swift/"+strMainClass_+"/"+strName_).toURI().toURL();
			for (String dependency : dependencies) {
				//logger_.info("/home/swift/"+strHandlerClassName_+"/"+dependency);
				searchPath[index++] = new File("/home/swift/"+strMainClass_+"/"+dependency).toURI().toURL();
			}
			
			ClassLoader cl = new URLClassLoader(searchPath);			
			Class<?> c = Class.forName(strHandlerClassName, true, cl);

			microcontroller = (IMicrocontroller) c.newInstance();
			logger_.info("HANDLER LOADED: "+strName_+" with dependencies: "+strDependencies_);

		} catch (Exception e) {
			logger_.error(strName_ + ": Failed to load handler class "
					+ strHandlerClassName + " class path is "
					+ System.getProperty("java.class.path"));
			logger_.error(strName_ + ": " + e.getStackTrace().toString());			
		}
		return microcontroller;
	}

	public Api(FileDescriptor log, FileDescriptor out, String name, String mainClass, 
						   String dependencies, Map<String, String> object_md, Map<String, String> req_md, 
						   SBus apiBus, String apiBusPath, Logger logger) 
	{
		logger_ = logger;
		strName_ = name;
		strMainClass_ = mainClass;
		strDependencies_ = dependencies;
		object_md_ = object_md;
		req_md_ = req_md;
		logFile_ = new MicrocontrollerLogger(log);  // !! Microcontroller log file
		out_pipe_ = new MicrocontrollerOutput(out, apiBus, apiBusPath); // !! DATA TO SWIFT MIDDLEWARE
		
		microcontroller_ = loadMicrocontroller(mainClass);
	}
	
	public MicrocontrollerLogger getLogFile() {
		return logFile_;
	}
	
	public MicrocontrollerOutput getOutPipe() {
		return out_pipe_;
	}

	public IMicrocontroller getMicrocontroller() {
		return microcontroller_;
	}
	
	public Map<String, String> getObjectMetadata() {
		return object_md_;
	}
	
	public Map<String, String> getRequestMetadata() {
		return req_md_;
	}

}