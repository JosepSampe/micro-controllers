package com.urv.storlet.transcoder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;

import org.openstack.storlet.common.*;


public class TesterDriver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		HashMap<String, String> md = new HashMap<String, String>();
		TranscoderStorlet transcoderStorlet = new TranscoderStorlet();
		
		System.out.println("Testing - PDF transcoder.");
	    try 
	    {
	        String strPathPrefix = "/home/evgenyl/workspace/Storlets/SystemTests/";
	      FileInputStream inputFile = new FileInputStream( new File( strPathPrefix + "example.pdf"));
	      FileOutputStream outFile  = new FileOutputStream(new File( strPathPrefix + "res.txt"));
	      FileOutputStream logFile  = new FileOutputStream(new File( strPathPrefix + "transcoderLogger.txt"));

	      StorletLogger logger = new StorletLogger(logFile.getFD());
	      StorletInputStream inputStream = new StorletInputStream(inputFile.getFD(), md);
	      StorletOutputStream outStream = new StorletOutputStream(outFile.getFD(), md);
	      ArrayList<StorletOutputStream> outStreams = new ArrayList<StorletOutputStream>();
	      outStreams.add(outStream);
	      ArrayList<StorletInputStream> inputStreams = new ArrayList<StorletInputStream>();
	      inputStreams.add(inputStream);
	      
	      transcoderStorlet.invoke(inputStreams, outStreams, md, logger);
	      
	      inputFile.close();
	      outFile.close();
	      logFile.close();
	      
	    } catch (IOException e) {
		     e.printStackTrace();
		     System.exit(1);
		} catch (StorletException e) {
			e.printStackTrace();
			System.exit(1);
		} finally {
			System.out.println("Done.");
		}
	}

}
