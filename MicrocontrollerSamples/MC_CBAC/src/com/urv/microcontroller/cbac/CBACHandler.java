package com.urv.microcontroller.cbac;

import com.urv.vertigo.api.Api;
import com.urv.vertigo.context.Context;
import com.urv.vertigo.microcontroller.IMicrocontroller;

/**
 * 
 * @author Josep Sampe
 * 
 */

public class CBACHandler implements IMicrocontroller {
	
	/***
	 * Microcontroller invoke method. 
	 */
	public void invoke(Context ctx, Api api) {
		
		ctx.logger.emitLog("Init CBAC Microcontroller");	

		String requetRoles = ctx.request.roles;
		String role = ctx.microcontroller.parameters.get("role");
		String allowed_cols = ctx.microcontroller.parameters.get("allowed_cols");
		
		ctx.logger.emitLog("User roles: "+requetRoles);
		ctx.logger.emitLog("Role: "+role+", Allowed columns: "+allowed_cols); 
		
		if (requetRoles.toLowerCase().contains(role)){
			ctx.logger.emitLog("--> Allowed request");
			api.storlet.set("adult-1.0.jar","select="+allowed_cols);
			api.storlet.run();
		} else {
			ctx.logger.emitLog("--> Unallowed request");
			ctx.request.cancel("ERROR: User not allowed");	
		}

		ctx.logger.emitLog("Ended CBAC Microcontroller");
		
	}

}
