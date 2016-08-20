/*============================================================================
 21-Oct-2015    josep.sampe       Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.daemon;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Future;


public class HandlerExecutionTask implements Runnable {
	private Logger logger_ = null;
	private ArrayList<Handler> handlerManager_ = null;
	private String taskId_ = null;
	private HashMap<String, Future> taskIdToTask_ = null;

	/*------------------------------------------------------------------------
	 * CTOR
	 * */
	public HandlerExecutionTask(ArrayList<Handler> handlerManager, Logger logger) {
		this.logger_ = logger;
		this.handlerManager_ = handlerManager;
		
		logger_.trace("Handler execution task created");
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
	 * Actual storlet invocation
	 * */
	@Override
	public void run() {

			logger_.trace("About to invoke handlers");
			
			for (Handler handler_obj : handlerManager_) {
				HandlerMetadata handlerMetadata = handler_obj.getMetadataFile();
				HandlerLogger handlerLogger = handler_obj.getLogFile();
				HandlerOutput out_md = handler_obj.getOutPipe();
				Map<String, String> file_md = handler_obj.getFileMetadata();
				Map<String, String> req_md = handler_obj.getReqMetadata();
				IHandler handler = handler_obj.getHandler();
				
				handler.invoke(handlerLogger, handlerMetadata, file_md, req_md, out_md);
				
				handlerMetadata.Flush();
				handlerLogger.Flush();
			}
			
			logger_.trace("Handlers invocation done");

			synchronized (taskIdToTask_) {
				taskIdToTask_.remove(taskId_);
			}
	}
}
/* ============================== END OF FILE =============================== */