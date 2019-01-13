package com.urv.vertigo.mc.transgrep;

import com.urv.vertigo.api.Api;
import com.urv.vertigo.context.Context;
import com.urv.vertigo.microcontroller.IMicrocontroller;

/**
 * 
 * @author Josep Sampe
 * 
 */

public class TransGrepHandler implements IMicrocontroller {
	
	/***
	 * MicroController invoke method. 
	 */
	public void invoke(Context ctx, Api api) {
		ctx.logger.emitLog("Init TransGrep MicroController");

		api.storlet.set("transcoder-1.0.jar", null);
		api.storlet.set("grep-1.0.jar","regexp=*^a*");
		api.storlet.run();

		ctx.logger.emitLog("Ended TransGrep MicroController");
	}
	
}
