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
		api.logger.emitLog("Init Prefetching Microcontroller");

		api.request.forward(); // Return request to the user; the rest of code will be executed asynchronously

		String resources = api.object.metadata.get("resources");
		api.logger.emitLog("Resources: "+resources);
				
		if (resources != null){
			List<String> staticResources = Arrays.asList(resources.split(","));
	
			for (String resource : staticResources){
				api.swift.prefetch(resource);
			}
		}
		
		api.logger.emitLog("Ended Prefetching Microcontroller");
	}
}