/*============================================================================
 18-Aug-2016    josep.sampe			Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.api;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import org.json.simple.JSONObject;
import org.slf4j.Logger;


public class ApiStorlet {
	private FileOutputStream stream;
	private Logger logger_;
	private JSONObject outMetadata = new JSONObject();
	private JSONObject storletList = new JSONObject();
	
	public ApiStorlet(FileDescriptor fd, Logger logger) {
		stream = new FileOutputStream(fd);
		logger_ = logger;
		
		logger_.trace("ApiStorlet created");
	}

	@SuppressWarnings("unchecked") 
	public void set(int i, String storlet, String parameters, String server){
		JSONObject storletPack = new JSONObject();
		storletPack.put("storlet",storlet);
		storletPack.put("params",parameters);
		storletPack.put("server",server);		
		storletList.put(i,storletPack);
	}
	
	@SuppressWarnings("unchecked") 
	public void run() {
		try {
			if (storletList.isEmpty()){
				outMetadata.put("command","CONTINUE");
			} else {
				outMetadata.put("command","STORLET");
				outMetadata.put("list",storletList);
			}
			stream.write(outMetadata.toString().getBytes());
			this.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	private void flush() {
		try {
			stream.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}