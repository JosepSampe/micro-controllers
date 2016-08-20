/*============================================================================
 18-Aug-2016    josep.sampe			Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.api;

import java.io.IOException;
import java.util.HashMap;

import org.slf4j.Logger;

import com.ibm.storlet.sbus.SBus;
import com.ibm.storlet.sbus.SBusDatagram;

public class ApiExternalBus {
	private SBus apiBus;
	private String apiBusPath; 
	private String token;
	private Logger logger_;
	private HashMap<String, String> parameters = new HashMap<String, String>();
	
	public ApiExternalBus(SBus apiSBus, String stringApiBusPath, String requestToken, Logger logger) {
		apiBus = apiSBus; 
		apiBusPath = stringApiBusPath;
		token = requestToken;
		logger_ = logger;
		
		logger_.trace("ApiExternalBus created");
	}	
	
	public void setParam(String key, String value){
		parameters.put(key, value);
	}

	public void sendCommand(String service, String command){

		parameters.put("service", service);
		parameters.put("op", command);
		parameters.put("token", token);

		SBusDatagram dtg = new SBusDatagram();
		dtg.setCommand(SBusDatagram.eStorletCommand.SBUS_CMD_EXECUTE);
		dtg.setExecParams(parameters);

		logger_.trace("Sending command to API");
		
		try {
			apiBus.send(apiBusPath, dtg);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}