/*============================================================================
 19-Oct-2015    josep.sampe		Initial implementation.
 03-Feb-2016	josep.sampe		Added bus to Internal Client
 17-Aug-2016	josep.sampe		Refactor
 ===========================================================================*/
package com.urv.vertigo.microcontroller;

import com.urv.vertigo.api.Api;
import org.slf4j.Logger;
import java.util.List;
import java.util.Arrays;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;


public class Microcontroller {

	private Logger logger_;
	private Api api_;
	private String strName_;
	private String strMainClass_;
	private String strDependencies_;
	private IMicrocontroller microcontroller_;

	private IMicrocontroller loadMicrocontroller() {
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
			Class<?> c = Class.forName(strMainClass_, true, cl);

			microcontroller = (IMicrocontroller) c.newInstance();
			logger_.info("MICROCONTROLLER LOADED: "+strName_+" with dependencies: "+strDependencies_);

		} catch (Exception e) {
			logger_.error(strName_ + ": Failed to load handler class "
					+ strMainClass_ + " class path is "
					+ System.getProperty("java.class.path"));
			logger_.error(strName_ + ": " + e.getStackTrace().toString());			
		}
		return microcontroller;
	}

	public Microcontroller(String name, String mainClass, String dependencies, Api api, Logger logger) {
		logger_ = logger;
		strName_ = name;
		strMainClass_ = mainClass;
		strDependencies_ = dependencies;
		api_ = api; 
		
		microcontroller_ = loadMicrocontroller();
	}
	
	public IMicrocontroller getMicrocontroller() {
		return microcontroller_;
	}
	
	public Api getApi(){
		return api_;	
	}
}
