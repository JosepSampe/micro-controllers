package com.urv.vertigo.mc.noop;

import com.urv.vertigo.api.Api;
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
	public void invoke(Api api) {
		api.logger.emitLog("*** Init Noop Microcontroller ***");
		api.request.forward();
		api.logger.emitLog("Ended Noop Microcontroller");
	}
	
}
