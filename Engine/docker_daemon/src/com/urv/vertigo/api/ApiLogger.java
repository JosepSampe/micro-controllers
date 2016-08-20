/*============================================================================
 18-Aug-2016    josep.sampe       Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.api;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;

import org.slf4j.Logger;

public class ApiLogger {
	private Logger logger_;
	private FileOutputStream stream;

	public ApiLogger(FileDescriptor fd, Logger logger) {
		stream = new FileOutputStream(fd);
		logger_ = logger;
		
		logger_.trace("ApiLogger created");
	}

	public void emitLog(String message) {
		message = message + "\n";
		try {
			stream.write(message.getBytes());
		} catch (IOException e) {

		}

	}

	public void flush() {
		try {
			stream.flush();
		} catch (IOException e) {
		}
	}

}