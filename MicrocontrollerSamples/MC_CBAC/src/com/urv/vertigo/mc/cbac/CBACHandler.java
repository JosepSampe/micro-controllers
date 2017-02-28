package com.urv.vertigo.mc.cbac;

import com.urv.vertigo.api.Api;
import com.urv.vertigo.microcontroller.IMicrocontroller;

public class CBACHandler implements IMicrocontroller {
	
	/***
	 * Microcontroller invoke method. 
	 */
	public void invoke(Api api) {
		
		api.logger.emitLog("Init CBAC Microcontroller");	

		String requetRoles = api.request.roles;
		String role = api.microcontroller.metadata.get("role").toString();
		String allowed_cols = api.microcontroller.metadata.get("allowed_cols").toString();
		
		api.logger.emitLog("User roles: "+requetRoles);
		api.logger.emitLog("Role: "+role+", Allowed columns: "+allowed_cols); 
		
		if (requetRoles.toLowerCase().contains(role)){
			api.logger.emitLog("--> Allowed request");
			api.storlet.set(0,"adult-1.0.jar","select="+allowed_cols,"object");
			api.storlet.run();
		} else {
			api.logger.emitLog("--> Unallowed request");
			api.request.cancel("ERROR: User not allowed");	
		}

		api.logger.emitLog("Ended CBAC Microcontroller");
		
	}

}
