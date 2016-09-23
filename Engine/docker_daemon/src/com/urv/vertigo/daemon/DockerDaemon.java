/*============================================================================
 20-Oct-2015    josep.sampe       	Initial implementation.
 17-Aug-2016	josep.sampe			Refactor
 ===========================================================================*/
package com.urv.vertigo.daemon;

import com.urv.vertigo.microcontroller.Microcontroller;
import com.urv.vertigo.microcontroller.MicrocontrollerExecutionTask;
import com.urv.vertigo.api.Api;

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
public class DockerDaemon {

	private static ch.qos.logback.classic.Logger logger_;
	private static SBus bus_;
	private static SBus apiBus_;
	private static String apiBusPath_;
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
			logger_ = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger("DockerDaemon");
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
		String strBusPath = args[0];
		apiBusPath_ = args[1];
		String strLogLevel = args[2];
		int nPoolSize = Integer.parseInt(args[3]);
		String strContId = args[4];
		
		if (initLog(strLogLevel) == false)
			return;

		logger_.trace("Instanciating Bus");
		bus_ = new SBus(strContId);
		apiBus_ = new SBus(strContId);

		try {
			logger_.trace("Initialising Swift and API bus");
			bus_.create(strBusPath);
			apiBus_.create(apiBusPath_);
		} catch (IOException e) {
			logger_.error("Failed to create Swift and API Bus");
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
				logger_.trace("listening on Bus");
				bus_.listen();
				logger_.trace("Bus listen() returned");
			} catch (IOException e) {
				logger_.error("Failed to listen on Bus");
				doContinue = false;
				break;
			}

			logger_.trace("Calling receive");
			SBusDatagram dtg = null;
			try {
				dtg = bus_.receive();
				logger_.trace("Receive returned");
			} catch (IOException e) {
				logger_.error("Failed to receive data on Bus");
				doContinue = false;
				break;

			}

			// We have the request
			doContinue = processDatagram(dtg);
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

		FileDescriptor toSwift = null;
		FileDescriptor logFd  = null;
		
		Map<String, String> object_md = null;
		Map<String, String> req_md = null;
		
		String mcName, mcMainClass, mcDependencies = null;
		Api api = null;
		Microcontroller mc = null;
		HashMap<String, FileDescriptor> mcLog = new HashMap<String,FileDescriptor>();
		ArrayList<Microcontroller> mcManager = new ArrayList<Microcontroller>();

		
		HashMap<String, String>[] FilesMD = dtg.getFilesMetadata();
		logger_.trace("Got " + nFiles + " fds");

		for (int i = 0; i < nFiles; ++i) {	
			String strFDtype = FilesMD[i].get("type");
			
			if (strFDtype.equals("SBUS_FD_OUTPUT_OBJECT")) {
				toSwift = dtg.getFiles()[i];
				logger_.trace("Got Microcontroller output fd");
				
			} else if (strFDtype.equals("SBUS_FD_INPUT_OBJECT")){
				//Isn't need to get fd
				JSONObject jsonMetadata = (JSONObject)new JSONParser().parse(FilesMD[i].get("json_md"));
				//Cast to MAP
				object_md = (Map<String, String>) jsonMetadata.get("object_md");
				req_md = (Map<String, String>) jsonMetadata.get("req_md");
				logger_.trace("Got object and request metadata");
	
			} else if (strFDtype.equals("SBUS_FD_LOGGER")){
				logFd = dtg.getFiles()[i];
				mcName = FilesMD[i].get("microcontroller");
				mcLog.put(mcName, logFd);
				mcMainClass = FilesMD[i].get("main");
				mcDependencies = FilesMD[i].get("dependencies");
				logger_.trace("Got logger microcontroller fd for "+mcName);
				
				api = new Api(mcName, mcLog.get(mcName), toSwift, object_md, req_md, logger_);				
				mc = new Microcontroller(mcName, mcMainClass, mcDependencies, api, logger_);
				mcManager.add(mc);
	
				logger_.trace("Microcontroller '"+mcName+"' created");
			}
		}
		
		//Going to execute microcontreoller(s) in a thread
		MicrocontrollerExecutionTask mcTask = new MicrocontrollerExecutionTask(mcManager, logger_);
		Future futureTask = threadPool_.submit((MicrocontrollerExecutionTask) mcTask);
		String taskId = futureTask.toString().split("@")[1];
		
		mcTask.setTaskIdToTask(taskIdToTask_);
		mcTask.setTaskId(taskId);
		logger_.trace("Microcontroller task ID is "+taskId);
		
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
