package com.urv.storlet.grep;

import org.openstack.storlet.common.*;

import java.io.*;
import java.util.*;

import org.apache.commons.compress.utils.IOUtils;

import org.grep4j.core.model.Profile;
import org.grep4j.core.model.ProfileBuilder;
import org.grep4j.core.result.GrepResults;

import static org.grep4j.core.Grep4j.grep;
import static org.grep4j.core.Grep4j.regularExpression;
import static org.grep4j.core.fluent.Dictionary.on;

/**
 * 
 * @author Josep Sampe
 *
 */

public class GrepStorlet implements IStorlet {
	
	/***
	 * Storlet invoke method
	 */
    public void invoke(ArrayList<StorletInputStream> inStreams, ArrayList<StorletOutputStream> outStreams, Map<String, String> parameters, StorletLogger logger) throws StorletException {
        
    	// CALCULATE TIME ------------
		long startTime, estimatedTime;
		startTime = System.nanoTime();
    	// ---------------------------

    	//logger.emitLog("Wellcome to Grep storlet");
        logger.emitLog("Going to read input stream");
        StorletInputStream sis = inStreams.get(0);
		InputStream inputStream = sis.getStream();
		HashMap<String, String> metadata = sis.getMetadata();
        
        //GET PARAMETERS
		logger.emitLog("Going to read parameters");
        String regexp = parameters.get("regexp");
        
        //PUT METADATA
        logger.emitLog("Adding regexp to the metadata");
        metadata.put("regexp", regexp);
   
        // READ OUTPUT STREAM
        logger.emitLog("Going to read output Stream");
		StorletObjectOutputStream storletObjectOutputStream = (StorletObjectOutputStream)outStreams.get(0);
		storletObjectOutputStream.setMetadata(metadata);
		OutputStream outputStream = storletObjectOutputStream.getStream();
 
		
        try {
        	Process p;
        	p = Runtime.getRuntime().exec("mkdir /tmp/input");
			p.waitFor();
			// copy input stream to file
			OutputStream objectFile = new FileOutputStream(new File("/tmp/input/object.data"));
			IOUtils.copy(inputStream, objectFile);
		
        } catch (UnsupportedEncodingException e) {
			//logger.emitLog("Grep - raised UnsupportedEncodingException: " + e.getMessage());
			throw new StorletException(e.getMessage());
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
        
        
		Profile localProfile = ProfileBuilder.newBuilder()
                .name("Data")
                .filePath("/tmp/input/object.data")
                .onLocalhost()
                .build();
		
        try {
        	
            GrepResults results = grep(regularExpression(regexp), on(localProfile));
            outputStream.write(("Grep results : \n\n" + results+"\n").getBytes());
            outputStream.write(("Total lines found : " + results.totalLines()+"\n").getBytes());
            outputStream.write(("Total Grep Execution Time : " + results.getExecutionTime() +"ns\n\n" ).getBytes());		
	        
            outputStream.close();
	        inputStream.close();
	        
            // CALCULATE TIME ----------
            estimatedTime = System.nanoTime() - startTime;			
            logger.emitLog("Total Execution Time : " + estimatedTime); 
            //elapsedTimeInSeconds = TimeUnit.MILLISECONDS.convert(estimatedTime, TimeUnit.NANOSECONDS) / 1000.0;
            //logger.emitLog("Total Execution Time : " + elapsedTimeInSeconds+"s" ); 
			//--------------------------
	              
		} catch (UnsupportedEncodingException e) {
			logger.emitLog("Grep - raised UnsupportedEncodingException: " + e.getMessage());
			throw new StorletException(e.getMessage());
		} catch (IOException e) {
			logger.emitLog("Grep - raised IOException: " + e.getMessage());
			throw new StorletException(e.getMessage());
		}
    }
}
