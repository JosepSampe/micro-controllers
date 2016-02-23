/*============================================================================
 19-Oct-2015    josep.sampe			Initial implementation.
 03-Feb-2016	josep.sampe			Added bus to Internal Client
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

public class Handler {

	private Logger logger_;

	private HandlerMetadata metadataFile_;
	private HandlerLogger logFile_;
	private HandlerOutput out_md_pipe_;
	private String strHandlerClassName_;
	private String strHandlerName_;
	private String strDependencies_;
	private IHandler handler_;
	private Map<String, String> file_md_;
	private Map<String, String> req_md_;


	private IHandler loadHandler(final String strHandlerClassName) {
		IHandler handler = null;
		List<String> dependencies = Arrays.asList(strDependencies_.split(","));
		URL[] searchPath = new URL[dependencies.size()+1];
		Integer index = 0;	

		try {	
			//logger_.info("/home/swift/"+strHandlerClassName_+"/"+strHandlerName_);
			
			searchPath[index++] = new File("/home/swift/"+strHandlerClassName_+"/"+strHandlerName_).toURI().toURL();
			for (String dependency : dependencies) {
				//logger_.info("/home/swift/"+strHandlerClassName_+"/"+dependency);
				searchPath[index++] = new File("/home/swift/"+strHandlerClassName_+"/"+dependency).toURI().toURL();
			}
			
			ClassLoader cl = new URLClassLoader(searchPath);			
			Class<?> c = Class.forName(strHandlerClassName, true, cl);

			handler = (IHandler) c.newInstance();
			logger_.info("HANDLER LOADED: "+strHandlerName_+" with dependencies: "+strDependencies_);

		} catch (Exception e) {
			logger_.error(strHandlerName_ + ": Failed to load handler class "
					+ strHandlerClassName + " class path is "
					+ System.getProperty("java.class.path"));
			logger_.error(strHandlerName_ + ": " + e.getStackTrace().toString());			
		}
		return handler;
	}

	public Handler(FileDescriptor metadataFD,FileDescriptor log, FileDescriptor out_md, String name, String mainClass, String dependencies, Map<String, String> file_md, Map<String, String> req_md, SBus icbus, String icPipePath, Logger logger) {
		logger_ = logger;
		strHandlerName_ = name;
		strHandlerClassName_ = mainClass;
		strDependencies_ = dependencies;
		file_md_ = file_md;
		req_md_ = req_md;
		metadataFile_ = new HandlerMetadata(metadataFD);
		logFile_ = new HandlerLogger(log);
		out_md_pipe_ = new HandlerOutput(out_md, icbus, icPipePath); // !! DATA TO SWIFT MIDDLEWARE
		
		handler_ = loadHandler(mainClass);
	}

	
	public HandlerMetadata getMetadataFile() {
		return metadataFile_;
	}
		
	public HandlerLogger getLogFile() {
		return logFile_;
	}
	
	public HandlerOutput getOutPipe() {
		return out_md_pipe_;
	}
	

	public IHandler getHandler() {
		return handler_;
	}
	
	public Map<String, String> getFileMetadata() {
		return file_md_;
	}
	
	public Map<String, String> getReqMetadata() {
		return req_md_;
	}

}