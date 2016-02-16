/*============================================================================
 21-Oct-2015    josep.sampe       Initial implementation.
 ===========================================================================*/
package com.urv.controller.daemon;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;

public class HandlerLogger {
	private FileOutputStream stream;

	public HandlerLogger(FileDescriptor fd) {
		stream = new FileOutputStream(fd);
	}

	public void emitLog(String message) {
		message = message + "\n";
		try {
			stream.write(message.getBytes());
		} catch (IOException e) {

		}

	}

	public void Flush() {
		try {
			stream.flush();
		} catch (IOException e) {
		}
	}

}