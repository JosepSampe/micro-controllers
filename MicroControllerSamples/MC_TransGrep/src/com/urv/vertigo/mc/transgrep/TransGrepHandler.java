/*============================================================================
 27-Jan-2016    josep.sampe       	Initial implementation.
 18-Aug-2016	josep.sampe			New implementation
 ===========================================================================*/
package com.urv.vertigo.mc.transgrep;

import com.urv.vertigo.api.Api;
import com.urv.vertigo.microcontroller.IMicrocontroller;

public class TransGrepHandler implements IMicrocontroller {
	
	/***
	 * MicroController invoke method. 
	 */
	public void invoke(Api api) {
		api.logger.emitLog("*** Init TransGrep MicroController ***");
		
		api.storlet.set(0,"transcoder-1.0.jar","","object");
		api.storlet.set(1,"grep-1.0.jar","regexp=*^a*","proxy");
		api.storlet.run();
		
		api.logger.emitLog("--- End TransGrep MicroController ---");
	}
	
}