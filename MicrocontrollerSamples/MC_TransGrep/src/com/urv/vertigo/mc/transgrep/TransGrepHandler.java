package com.urv.vertigo.mc.transgrep;

import com.urv.vertigo.api.Api;
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
	public void invoke(Api api) {
		api.logger.emitLog("Init TransGrep MicroController");

		api.storlet.set("transcoder-1.0.jar", null);
		api.storlet.set("grep-1.0.jar","regexp=*^a*");
		api.storlet.run();

		api.logger.emitLog("Ended TransGrep MicroController");
	}
	
}
