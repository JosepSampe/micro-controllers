package com.urv.microcontroller.noopstorlet;

import com.urv.vertigo.api.Api;
import com.urv.vertigo.context.Context;
import com.urv.vertigo.microcontroller.IMicrocontroller;

/**
 * 
 * @author Josep Sampe
 * 
 */

public class NoopStorletHandler implements IMicrocontroller {
	
	/***
	 * Microcontroller invoke method. 
	 */
	public void invoke(Context ctx, Api api) {
		ctx.logger.emitLog("*** Init NoopStorlet Microcontroller ***");
		api.storlet.set("noop-1.0.jar");
		api.storlet.run();
		ctx.logger.emitLog("Ended NoopStorlet Microcontroller");
	}
	
}
