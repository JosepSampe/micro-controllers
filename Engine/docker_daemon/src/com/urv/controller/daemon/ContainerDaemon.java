/*============================================================================
 20-Oct-2015    josep.sampe       Initial implementation.
 ===========================================================================*/
package com.urv.controller.daemon;

import java.io.FileDescriptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.ibm.storlet.sbus.*;

import java.util.concurrent.*;

/*----------------------------------------------------------------------------
 * CDaemon
 *  
 * */
public class ContainerDaemon {

	private static ch.qos.logback.classic.Logger logger_;
	private static SBus sbus_;
	private static SBus icbus_;
	private static String icPipePath_;
	private static ExecutorService threadPool_;
	private static HashMap<String, Future> taskIdToTask_;
	private static int nDefaultTimeoutToWaitBeforeShutdown_ = 3;

	/*------------------------------------------------------------------------
	 * initLog
	 * */
	private static boolean initLog(final String strLogLevel) {
		Level newLevel = Level.toLevel(strLogLevel);
		boolean bStatus = true;
		try {
			logger_ = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("ControllerDaemon");
			logger_.setLevel(newLevel);
			logger_.info("Logger Started");
		} catch (Exception e) {
			System.err.println("got exception " + e);
			bStatus = false;
		}
		return bStatus;
	}

	/*------------------------------------------------------------------------
	 * main
	 * 
	 * Entry point.
	 * args[1] - path to SBus
	 * args[2] - log level
	 * args[3] - thread pool size
	 * 
	 * */
	public static void main(String[] args) throws Exception {
		initialize(args);
		mainLoop();
		exit();
	}

	/*------------------------------------------------------------------------
	 * initialize
	 * 
	 * Initialize the resources
	 * */
	private static void initialize(String[] args) throws Exception {
		String strSBusPath = args[0];
		icPipePath_ = args[1];
		String strLogLevel = args[2];
		int nPoolSize = Integer.parseInt(args[3]);
		String strContId = args[4];
		
		if (initLog(strLogLevel) == false)
			return;

		logger_.trace("Instanciating SBus");
		sbus_ = new SBus(strContId);
		icbus_ = new SBus(strContId);
		// CREAR SBUS HACIA INTERNAL CLIENT
		
		
		try {
			logger_.trace("Initialising SBus");
			sbus_.create(strSBusPath);
			icbus_.create(icPipePath_);
		} catch (IOException e) {
			logger_.error("Failed to create SBus");
			return;
		}
		logger_.trace("Initialising thread pool with " + nPoolSize + " threads");
		threadPool_ = Executors.newFixedThreadPool(nPoolSize);
		taskIdToTask_ = new HashMap<String, Future>();
	}

	/*------------------------------------------------------------------------
	 * mainLoop
	 * 
	 * The main loop - listen, receive, execute till the HALT command. 
	 * */
	private static void mainLoop() throws Exception {
		boolean doContinue = true;
		while (doContinue) {
			// Wait for incoming commands
			try {
				logger_.trace("listening on SBus");
				sbus_.listen();
				logger_.trace("SBus listen() returned");
			} catch (IOException e) {
				logger_.error("Failed to listen on SBus");
				doContinue = false;
				break;
			}

			logger_.trace("Calling receive");
			SBusDatagram dtg = null;
			try {
				dtg = sbus_.receive();
				logger_.trace("Receive returned");
			} catch (IOException e) {
				logger_.error("Failed to receive data on SBus");
				doContinue = false;
				break;

			}

			// We have the request
			doContinue = processDatagram(dtg); // --> THREAD??
		}
	}

	/*------------------------------------------------------------------------
	 * processDatagram
	 * 
	 * Analyze the request datagram.
	 * 
	 * */
	
	@SuppressWarnings("unchecked")
	private static boolean processDatagram(SBusDatagram dtg) throws IOException, ParseException {
		int nFiles = dtg.getNFiles();

		FileDescriptor response_fd = null; 
		FileDescriptor metadata_file_fd = null;
		FileDescriptor logger_file_fd  = null;
		
		Map<String, String> file_md = null;
		Map<String, String> req_md = null;
		
		String handlerName,mainClass,dependencies = null;
		HashMap<String, FileDescriptor> handlerLog = new HashMap<String,FileDescriptor>();
		ArrayList<Handler> handlerManager = new ArrayList<Handler>();

		
		HashMap<String, String>[] FilesMD = dtg.getFilesMetadata();
		logger_.trace("Got " + nFiles + " fds");

		for (int i = 0; i < nFiles; ++i) {	
			String strFDtype = FilesMD[i].get("type");
			
			if (strFDtype.equals("SBUS_FD_OUTPUT_OBJECT")) {
				response_fd = dtg.getFiles()[i];			
				logger_.trace("Got output fd");
				
			} else if (strFDtype.equals("SBUS_FD_INPUT_OBJECT")){
				//Isn't need to get fd
				logger_.trace("Got file and request metadata");
				JSONObject jsonMetadata = (JSONObject)new JSONParser().parse(FilesMD[i].get("json_md"));

				//Cast to MAP
				file_md = (Map<String, String>) jsonMetadata.get("file_md");
				req_md = (Map<String, String>) jsonMetadata.get("req_md");
	
			} else if (strFDtype.equals("SBUS_FD_LOGGER")){
				logger_file_fd = dtg.getFiles()[i];
				handlerName = FilesMD[i].get("handler");
				handlerLog.put(handlerName,logger_file_fd);
				logger_.trace("Got logger handler fd for "+handlerName);
			
			} else if (strFDtype.equals("SBUS_FD_OUTPUT_OBJECT_METADATA")){
				metadata_file_fd = dtg.getFiles()[i];
				handlerName = FilesMD[i].get("handler");
				mainClass = FilesMD[i].get("main");
				dependencies = FilesMD[i].get("dependencies");
				
				logger_.trace("Got metadata handler fd for "+handlerName);
				
				//Note: All log fds are loaded
				handlerManager.add(new Handler(metadata_file_fd,handlerLog.get(handlerName),response_fd, handlerName,mainClass,dependencies, file_md, req_md, icbus_, icPipePath_, logger_));
	
			}
		}
		
		//Going to execute handler(or handlers) in a thread
		
		HandlerExecutionTask hTask = new HandlerExecutionTask(handlerManager,logger_);
		Future futureTask = threadPool_.submit((HandlerExecutionTask) hTask);
		String taskId = futureTask.toString().split("@")[1];
		
		hTask.setTaskIdToTask(taskIdToTask_);
		hTask.setTaskId(taskId);
		logger_.trace("Handler task ID is "+taskId);
		
		synchronized (taskIdToTask_) {
			taskIdToTask_.put(taskId, futureTask);
		}
		

		return true;
	}
	 
	/*------------------------------------------------------------------------
	 * exit
	 * 
	 * Release the resources and quit
	 * */
	private static void exit() {
		logger_.info("Daemon is going down...shutting down threadpool");
		try {
			threadPool_.awaitTermination(nDefaultTimeoutToWaitBeforeShutdown_,
					TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		threadPool_.shutdown();
		logger_.info("threadpool down");
	}
}
/* ============================== END OF FILE =============================== */