/*============================================================================
 20-Jan-2015    josep.sampe	Initial implementation.
 18-Aug-2016	josep.sampe	New implementation
 ===========================================================================*/
package com.urv.vertigo.mc.counter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
		
		java.util.Date date = new java.util.Date();
		SimpleDateFormat formater = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zz");
		String strDate = formater.format(date);
		
		// Object Metadata (X-Object-Meta)
		Long accessed = api.object.metadata.incr("Accessed");
		api.object.metadata.set("Last-Access", strDate);
		
		// Microcontroller specific metadata
		api.microcontroller.metadata.put("accessed", accessed);
		api.microcontroller.metadata.put("last_access", strDate);
		api.microcontroller.updateMetadata();
		
		//if (accessed > 10)
		//	api.object.move("data_2/adult.csv");
		

		try {
			
			BufferedReader reader = api.object.get();
			api.logger.emitLog(reader.readLine());
			reader.close();

			
		} catch (IOException e) {
			api.logger.emitLog("Error");
		}
		

		api.logger.emitLog("---------- NEW INFORMATION ----------");
		api.logger.emitLog("Accessed: " + Long.toString(accessed));
		api.logger.emitLog("Last access: " + strDate);
		api.logger.emitLog("-------------------------------------");
		
		api.logger.emitLog("--- End Counter Microcontroller ---");

	}
	
}
