/*============================================================================
 20-Jan-2015    josep.sampe       	Initial implementation.
 18-Aug-2016	josep.sampe			New implementation
 ===========================================================================*/
package com.urv.vertigo.mc.counter;

import java.text.SimpleDateFormat;

import com.urv.vertigo.api.Api;
import com.urv.vertigo.microcontroller.IMicrocontroller;

public class CounterHandler implements IMicrocontroller {
	
	/***
	 * Microcontroller invoke method. 
	 */
	@SuppressWarnings("unchecked")
	public void invoke(Api api) {
		
		api.logger.emitLog("*** Init Counter Microcontroller ***");
		
		api.request.forward(); // Return request to the user; the rest of code will be executed asynchronously
		
		int accessed = Integer.parseInt(api.microcontroller.metadata.get("accessed").toString())+1;
		java.util.Date date = new java.util.Date();
		SimpleDateFormat formater = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss zz");
		String strDate = formater.format(date);

		// Microcontroller specific metadata
		api.microcontroller.metadata.put("accessed",accessed);
		api.microcontroller.metadata.put("last_access", strDate);
		api.microcontroller.updateMetadata();
		
		// Object Metadata (X-Object-Meta)
		api.object.setMetadata("Accessed", Integer.toString(accessed));
		api.object.setMetadata("Last-Access", strDate);
		
		api.object.move("data_2/adult.csv");

		api.logger.emitLog("---------- NEW INFORMATION ----------");
		api.logger.emitLog("Accessed: " + Integer.toString(accessed));
		api.logger.emitLog("Last access: " + strDate);
		api.logger.emitLog("-------------------------------------");

		api.logger.emitLog("--- End Counter Microcontroller ---");

	}
	
}