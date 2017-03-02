package com.urv.vertigo.daemon;

import com.urv.vertigo.microcontroller.MicrocontrollerExecutionTask;
import java.io.IOException;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import com.ibm.storlet.sbus.*;
import java.util.concurrent.*;

/*----------------------------------------------------------------------------
 * VertigoDockerDaemon
 *  
 * */
public class DockerDaemon {

	private static ch.qos.logback.classic.Logger logger_;
	private static SBus bus_;
	private static SBus apiBus_;
	private static String apiBusPath_;
	private static ExecutorService threadPool_;
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

			//doContinue = processDatagram(dtg);
			MicrocontrollerExecutionTask mcTask = new MicrocontrollerExecutionTask(dtg, logger_);
			threadPool_.execute(mcTask);
		}
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
