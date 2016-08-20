/*============================================================================
 21-Oct-2015    josep.sampe       	Initial implementation.
 17-Aug-2016	josep.sampe			Refactor
 ===========================================================================*/
package com.urv.vertigo.microcontroller;

import com.urv.vertigo.api.Api;
import org.slf4j.Logger;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.Future;


public class MicrocontrollerExecutionTask implements Runnable {
	private Logger logger_ = null;
	private ArrayList<Microcontroller> mcManager_ = null;
	private String taskId_ = null;
	private HashMap<String, Future> taskIdToTask_ = null;

	/*------------------------------------------------------------------------
	 * CTOR
	 * */
	public MicrocontrollerExecutionTask(ArrayList<Microcontroller> mcManager, Logger logger) {
		this.logger_ = logger;
		this.mcManager_ = mcManager;
		
		logger_.trace("Microcontroller execution task created");
	}


	/*------------------------------------------------------------------------
	 * setters
	 * */
	public void setTaskId(String taskId) {
		taskId_ = taskId;
	}

	public void setTaskIdToTask(HashMap<String, Future> taskIdToTask) {
		taskIdToTask_ = taskIdToTask;
	}

	/*------------------------------------------------------------------------
	 * run
	 * 
	 * Actual microcontroller invocation
	 * */
	@Override
	public void run() {

			logger_.trace("About to invoke microcontroller");
			
			for (Microcontroller mc_obj : mcManager_) {
				IMicrocontroller microcontroller = mc_obj.getMicrocontroller();
				Api api = mc_obj.getApi();				
				microcontroller.invoke(api);
				api.logger.flush();
			}
			
			logger_.trace("Microcontroller invocation done");

			synchronized (taskIdToTask_) {
				taskIdToTask_.remove(taskId_);
			}
	}
}