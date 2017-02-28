package com.urv.vertigo.api;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import org.json.simple.JSONObject;
import org.slf4j.Logger;


public class ApiStorlet {
	private FileOutputStream stream;
	private Logger logger_;
	private Integer index;
	private JSONObject outMetadata = new JSONObject();
	private JSONObject storletList = new JSONObject();
	
	public ApiStorlet(FileDescriptor fd, Logger logger) {
		stream = new FileOutputStream(fd);
		logger_ = logger;
		index = 0;
		logger_.trace("ApiStorlet created");
	}

	@SuppressWarnings("unchecked") 
	public void set(String storlet, String parameters){
		JSONObject storletPack = new JSONObject();
		storletPack.put("storlet",storlet);
		storletPack.put("params",parameters);
		storletPack.put("server","object");		
		storletList.put(index,storletPack);
		index = index+1;
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