package com.urv.vertigo.mc.noop;

import com.urv.vertigo.api.Api;
import com.urv.vertigo.context.Context;
import com.urv.vertigo.microcontroller.IMicrocontroller;

/**
 * 
 * @author Josep Sampe
 * 
 */

public class NoopHandler implements IMicrocontroller {
	
	/***
	 * Microcontroller invoke method. 
	 */
	public void invoke(Context ctx, Api api) {
		ctx.logger.emitLog("*** Init Noop Microcontroller ***");
		ctx.request.forward();
		ctx.logger.emitLog("Ended Noop Microcontroller");
	}
	
}
