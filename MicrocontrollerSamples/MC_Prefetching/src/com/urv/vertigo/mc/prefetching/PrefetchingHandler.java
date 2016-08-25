/*============================================================================
 08-Feb-2016    josep.sampe	Initial implementation.
 24-Aug-2016    josep.sampe	New implementation.
 ===========================================================================*/
package com.urv.vertigo.mc.prefetching;

import java.util.Arrays;
import java.util.List;
import com.urv.vertigo.api.Api;
import com.urv.vertigo.microcontroller.IMicrocontroller;

public class PrefetchingHandler implements IMicrocontroller {
	
	/***
	 * Microcontroller invoke method. 
	 */
	public void invoke(Api api) {
		api.logger.emitLog("*** Init Prefetching Microcontroller ***");

		api.request.forward(); // Return request to the user; the rest of code will be executed asynchronously

		String resources = api.object.getMetadata("resources");
		List<String> staticResources = Arrays.asList(resources.split(","));

		api.logger.emitLog("Resources: "+staticResources.toString());
		
		for (String resource : staticResources)
			api.swift.prefetch(resource);

		api.logger.emitLog("--- End Prefetching Microcontroller ---");
	}
}