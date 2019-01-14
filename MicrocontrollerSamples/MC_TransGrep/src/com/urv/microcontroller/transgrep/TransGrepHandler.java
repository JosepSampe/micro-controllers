package com.urv.microcontroller.transgrep;

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
		ctx.logger.emitLog("Init TransGrep Micro-controller");

		api.storlet.set("transcoder-1.0.jar");
		api.storlet.set("grep-1.0.jar","regexp=*^a*");
		api.storlet.run();

		ctx.logger.emitLog("Ended TransGrep Micro-controller");
	}
	
}
