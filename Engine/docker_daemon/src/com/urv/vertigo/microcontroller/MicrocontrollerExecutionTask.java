package com.urv.vertigo.microcontroller;

import com.ibm.storlet.sbus.SBusDatagram;
import com.urv.vertigo.api.Api;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;

import java.io.FileDescriptor;
import java.util.HashMap;
import java.util.Map;


public class MicrocontrollerExecutionTask implements Runnable {
	private Logger logger_ = null;
	private SBusDatagram dtg = null;

	/*------------------------------------------------------------------------
	 * CTOR
	 * */
	public MicrocontrollerExecutionTask(SBusDatagram dtg, Logger logger) {
		this.logger_ = logger;
		//this.mc_ = mc;
		//this.api_ = api;
		
		this.dtg = dtg;
		
		logger_.trace("Microcontroller execution task created");
	}

	/*------------------------------------------------------------------------
	 * run
	 * 
	 * Actual microcontroller invocation
	 * */
	@SuppressWarnings("unchecked")
	public void run() {
		
		int nFiles = dtg.getNFiles();

		FileDescriptor toSwift = null;
		FileDescriptor logFd  = null;
		
		Map<String, String> object_md = null;
		Map<String, String> req_md = null;
		
		String mcName, mcMainClass, mcDependencies = null;
		Api api = null;
		Microcontroller mc = null;
		HashMap<String, FileDescriptor> mcLog = new HashMap<String,FileDescriptor>();

		
		HashMap<String, String>[] FilesMD = dtg.getFilesMetadata();
		logger_.trace("Got " + nFiles + " fds");

		for (int i = 0; i < nFiles; ++i) {	
			String strFDtype = FilesMD[i].get("type");
			
			if (strFDtype.equals("SBUS_FD_OUTPUT_OBJECT")) {
				toSwift = dtg.getFiles()[i];
				logger_.trace("Got Microcontroller output fd");
				
			} else if (strFDtype.equals("SBUS_FD_INPUT_OBJECT")){
				//Isn't need to get fd
				JSONObject jsonMetadata;
				try {
					jsonMetadata = (JSONObject)new JSONParser().parse(FilesMD[i].get("json_md"));
					object_md = (Map<String, String>) jsonMetadata.get("object_md");
					req_md = (Map<String, String>) jsonMetadata.get("req_md");
				} catch (ParseException e) {
					e.printStackTrace();
				}
				logger_.trace("Got object and request metadata");
	
			} else if (strFDtype.equals("SBUS_FD_LOGGER")){
				logFd = dtg.getFiles()[i];
				mcName = FilesMD[i].get("microcontroller");
				logger_.trace("Got logger microcontroller fd for "+mcName);
				mcLog.put(mcName, logFd);
				mcMainClass = FilesMD[i].get("main");
				mcDependencies = FilesMD[i].get("dependencies");
				
				logger_.trace("Going to create API");
				api = new Api(mcName, mcLog.get(mcName), toSwift, object_md, req_md, logger_);
				logger_.trace("Going to create Microcontroller");
				mc = new Microcontroller(mcName, mcMainClass, mcDependencies, logger_);

				logger_.trace("Microcontroller '"+mcName+"' loaded");
				IMicrocontroller microcontroller = mc.getMicrocontroller();				
				microcontroller.invoke(api);
				
				mc = null;
				api = null;
				microcontroller = null;
			}
		}					
	}
}