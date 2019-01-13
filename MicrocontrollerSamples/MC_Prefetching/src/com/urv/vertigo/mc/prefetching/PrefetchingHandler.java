package com.urv.vertigo.mc.prefetching;

import java.util.Arrays;
import java.util.List;
import com.urv.vertigo.api.Api;
import com.urv.vertigo.context.Context;
import com.urv.vertigo.microcontroller.IMicrocontroller;

/**
 * 
 * @author Josep Sampe
 * 
 */

public class PrefetchingHandler implements IMicrocontroller {
	
	/***
	 * Microcontroller invoke method. 
	 */
	public void invoke(Context ctx, Api api) {
		ctx.logger.emitLog("Init Prefetching Micro-controller");

		ctx.request.forward(); // Return request to the user; the rest of code will be executed asynchronously

		String resources = ctx.object.metadata.get("resources");
		ctx.logger.emitLog("Resources: "+resources);
				
		if (resources != null){
			List<String> staticResources = Arrays.asList(resources.split(","));
	
			for (String resource : staticResources){
				ctx.logger.emitLog(resource);
				api.swift.prefetch(resource);
			}
		}
		
		ctx.logger.emitLog("Ended Prefetching Micro-controller");
	}
}