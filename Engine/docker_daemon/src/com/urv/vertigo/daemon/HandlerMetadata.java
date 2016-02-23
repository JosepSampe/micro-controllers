/*============================================================================
 20-Oct-2015    josep.sampe       Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.daemon;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class HandlerMetadata {
	private FileInputStream inStream;
	private FileOutputStream outStream;
	private BufferedReader br;
	private FileDescriptor fdi;

	public HandlerMetadata(FileDescriptor fd) {
		fdi = fd;
		outStream = new FileOutputStream(fd);
	}

	
	public String getMetadata() {
		inStream = new FileInputStream(fdi);
		String sCurrentLine, lastLine = null;
		try {
			br = new BufferedReader(new InputStreamReader(inStream));

		        while ((sCurrentLine = br.readLine()) != null) {
				lastLine = sCurrentLine;
		        }

		} catch (IOException e) {

		}
		return lastLine;
	}

	public void appendMetadata(String message) {
		message = message + "\n";

		try {
			synchronized(outStream){
				outStream.write(message.getBytes());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	public void updateMetadata(String message){
		synchronized(outStream){
			try {
				outStream.getChannel().truncate(0);
				outStream.write(message.getBytes());		
			} catch (IOException e) {
				e.printStackTrace();
			}
		}	
	}

	public void Flush() {
		try {
			outStream.flush();
		} catch (IOException e) {
		}
	}

}