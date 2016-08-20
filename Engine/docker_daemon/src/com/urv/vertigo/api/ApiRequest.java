/*============================================================================
 21-Oct-2015    josep.sampe			Initial implementation.
 05-feb-2016	josep.sampe			Added Internal Client.
 17-Aug-2016	josep.sampe			Refactor 
 ===========================================================================*/
package com.urv.vertigo.daemon;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;


public class ApiRequest {
	private FileOutputStream stream;
	private JSONObject outMetadata = new JSONObject();
	
	public Map<String, String> metadata;

	public ApiRequest(FileDescriptor fd, Map<String, String> requestMetadata) {
		stream = new FileOutputStream(fd);
		metadata = requestMetadata;
	}

	@SuppressWarnings("unchecked") 
	public void set(int i, String storlet, String parameters, String server){
		JSONObject storletPack = new JSONObject();
		storletPack.put("storlet",storlet);
		storletPack.put("params",parameters);
		storletPack.put("server",server);		
		outMetadata.put(i,storletPack);
	}
	
	public String getList(){
		return outMetadata.toString();
	}

	public void run() {
		try {
			if (!outMetadata.isEmpty()){
				stream.write(outMetadata.toString().getBytes());
			} else {
				stream.write("{}".getBytes());
			}
			//this.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	public void Flush() {
		try {
			stream.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}