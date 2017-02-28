package com.urv.vertigo.mc.counter;

import java.text.SimpleDateFormat;
import com.urv.vertigo.api.Api;
import com.urv.vertigo.microcontroller.IMicrocontroller;

public class CounterHandler implements IMicrocontroller {
	
	/***
	 * Microcontroller invoke method. 
	 */
	public void invoke(Api api) {

		api.logger.emitLog("*** Init Counter Microcontroller ***");
		
		java.util.Date date = new java.util.Date();
		SimpleDateFormat formater = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zz");
		String strDate = formater.format(date);
		
		//long before = System.nanoTime();
		// Object Metadata (X-Object-Meta)
		Long accessed = api.object.metadata.incr("Accessed");
		api.object.metadata.set("Last-Access", strDate);

		// api.logger.emitLog(api.microcontroller.metadata.toString());
		
		// Microcontroller specific metadata
		// api.microcontroller.metadata.put("accessed", accessed);
		// api.microcontroller.metadata.put("last_access", strDate);
		// api.microcontroller.updateMetadata();
		
		if (accessed > 100){
			api.request.cancel("Error: maximum reads reached.");
			api.object.delete();
		} else
			api.request.forward();

		// long after = System.nanoTime();
		// api.logger.emitLog("Counter MC -- Elapsed [ms]: "+((after-before)/1000000.0f));
		api.logger.emitLog("---------- NEW INFORMATION ----------");
		api.logger.emitLog("Accessed: " + Long.toString(accessed));
		api.logger.emitLog("Last access: " + strDate);
		api.logger.emitLog("-------------------------------------");
		
		api.logger.emitLog("Ended Counter Microcontroller");

	}
	
}
