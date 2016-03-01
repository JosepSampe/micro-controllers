/*============================================================================
 27-Jan-2016    josep.sampe       Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.mc.transgrep;

import java.util.Map;

import com.urv.vertigo.daemon.*;

public class TransGrepHandler implements IHandler {
	/***
	 * MicroController invoke method. 
	 */
	
	@Override
	public void invoke( HandlerLogger logger, HandlerMetadata meta, Map<String, String> file_md, 
						Map<String, String> req_md,  HandlerOutput out) {
		logger.emitLog("*********** Init TransGrep MicroController ************");
		
		out.setStorlet(0,"transcoder-1.0.jar","","object");
		out.setStorlet(1,"grep-1.0.jar","regexp=*^a*","proxy");
		out.execStorlets();
		
		logger.emitLog("To execute:" + out.getStorletList());
		
		logger.emitLog("************ End TransGrep MicroController ************");
	}
}