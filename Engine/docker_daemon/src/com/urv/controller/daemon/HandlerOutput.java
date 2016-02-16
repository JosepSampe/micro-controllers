/*============================================================================
 21-Oct-2015    josep.sampe			Initial implementation.
 05-feb-2016	josep.sampe			Added Internal Client.
 ===========================================================================*/
package com.urv.controller.daemon;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;

import com.ibm.storlet.sbus.SBus;
import com.ibm.storlet.sbus.SBusDatagram;


public class HandlerOutput {
	private FileOutputStream out_fd;
	JSONObject outMetadata = new JSONObject();
	
	private SBus icbus;
	private String pipePath;
	HashMap<String, String> execParams = new HashMap<String, String>();
	
	public HandlerOutput(FileDescriptor fd, SBus icb, String pp) {
		out_fd = new FileOutputStream(fd);
		icbus = icb;
		pipePath = pp;
	}
	
	/*
	 * Storlets
	 */
	@SuppressWarnings("unchecked") 
	public void setStorlet(int i, String name, String parameters, String node){
		JSONObject storletPack = new JSONObject();
		JSONObject execStorlet = new JSONObject();
		execStorlet.put(name,parameters);
		storletPack.put("Storlet",execStorlet);
		storletPack.put("NodeToExecute",node);		
		outMetadata.put(i,storletPack);
	}
	
	public String getStorletList(){
		return outMetadata.toString();
	}

	public void execStorlets() {
		try {
			if (!outMetadata.isEmpty()){
				out_fd.write(outMetadata.toString().getBytes());
			} else {
				out_fd.write("{}".getBytes());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	public void Flush() {
		try {
			out_fd.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	/*
	 * internal Client
	 */
	public void setExecParam(String key, String value){
		execParams.put(key, value);
	}

	public void execCommand(String command, Map<String, String> req_md){
	
		execParams.put("op", command);
		execParams.put("swift_token", req_md.get("X-Auth-Token"));

		SBusDatagram dtg = new SBusDatagram();
		dtg.setCommand(SBusDatagram.eStorletCommand.SBUS_CMD_EXECUTE);
		dtg.setExecParams(execParams);

		try {
			icbus.send(pipePath,dtg);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}