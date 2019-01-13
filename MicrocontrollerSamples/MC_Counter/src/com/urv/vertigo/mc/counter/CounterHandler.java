package com.urv.vertigo.mc.counter;

import java.text.SimpleDateFormat;
import com.urv.vertigo.api.Api;
import com.urv.vertigo.context.Context;
import com.urv.vertigo.microcontroller.IMicrocontroller;

/**
 * 
 * @author Josep Sampe
 * 
 */

public class CounterHandler implements IMicrocontroller {
	
	/***
	 * Microcontroller invoke method. 
	 */
	public void invoke(Context ctx, Api api) {

		ctx.logger.emitLog("*** Init Counter Microcontroller ***");
		
		java.util.Date date = new java.util.Date();
		SimpleDateFormat formater = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zz");
		String strDate = formater.format(date);
		
		//long before = System.nanoTime();
		// Object Metadata (X-Object-Meta)
		Long accessed = ctx.object.metadata.incr("Accessed");
		ctx.object.metadata.set("Last-Access", strDate);

		// api.logger.emitLog(api.microcontroller.metadata.toString());
		
		// Microcontroller specific metadata
		// api.microcontroller.metadata.put("accessed", accessed);
		// api.microcontroller.metadata.put("last_access", strDate);
		// api.microcontroller.updateMetadata();
		
		if (accessed > 100){
			ctx.request.cancel("Error: maximum reads reached.");
			ctx.object.delete();
		} else
			ctx.request.forward();

		// long after = System.nanoTime();
		// api.logger.emitLog("Counter MC -- Elapsed [ms]: "+((after-before)/1000000.0f));
		ctx.logger.emitLog("---------- NEW INFORMATION ----------");
		ctx.logger.emitLog("Accessed: " + Long.toString(accessed));
		ctx.logger.emitLog("Last access: " + strDate);
		ctx.logger.emitLog("-------------------------------------");
		
		ctx.logger.emitLog("Ended Counter Microcontroller");

	}
	
}
