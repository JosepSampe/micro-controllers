package com.urv.storlet.scanhtml;


import java.io.IOException;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import org.openstack.storlet.common.*;

/**
 * 
 * @author Josep Sampe
 *
 */


@SuppressWarnings("unchecked")
public class ScanHtml implements IStorlet {
	
	/***
	 * Storlet invoke method
	 */
    public void invoke(ArrayList<StorletInputStream> inStreams, ArrayList<StorletOutputStream> outStreams, 
    				  Map<String, String> parameters, StorletLogger logger) throws StorletException {
        
    	
    	// CALCULATE TIME ------------
		long startTime, estimatedTime;
		startTime = System.nanoTime();
    	// ---------------------------
		
    	logger.emitLog("Wellcome to Scan Html storlet");
        //logger.emitLog("Going to read input stream");
        StorletInputStream sis = inStreams.get(0);
		InputStream inputStream = sis.getStream();
		//logger.emitLog("-> Done.");
		HashMap<String, String> metadata = sis.getMetadata();
        
        //GET PARAMETERS
		//logger.emitLog("Going to read parameters");
        String search_tags = parameters.get("search");
        //logger.emitLog("-> Done.");
        
        //PUT METADATA
        //logger.emitLog("Adding search tags to the metadata");
        metadata.put("search_tags", search_tags);
      
        // READ OUTPUT STREAM
        logger.emitLog("Going to read output Stream");
		StorletObjectOutputStream storletObjectOutputStream = (StorletObjectOutputStream)outStreams.get(0);
		OutputStream outputStream = storletObjectOutputStream.getStream();

	
        try {

        	Document doc = Jsoup.parse(inputStream,"UTF-8","");		
        	Set<String> prefetch_file_list = new HashSet<String>();
        	
        	ListIterator<Element> iter = doc.select(search_tags).listIterator();
            while(iter.hasNext()) {
                Element el = iter.next();              
                prefetch_file_list.add(el.attr("src"));
            }

            JSONObject output_data = new JSONObject();
            
            output_data.put("prefetch_file_list", prefetch_file_list.toString().replace("[", "").replace("]", "").replace(" ", ""));
            
	        metadata.put("search-tags", search_tags);
	        metadata.put("micro-controller-data", output_data.toString().replace("\\", ""));
	        metadata.put("run-micro-controller","WebPrefetching-1.0.jar");
	        	        	        
	        storletObjectOutputStream.setMetadata(metadata);
        
	        outputStream.write(doc.toString().getBytes());
	        
        	inputStream.close();
	        outputStream.close();
  
            // CALCULATE TIME ----------
            estimatedTime = System.nanoTime() - startTime;			
            logger.emitLog("Total Execution Time : " + estimatedTime); 
			//--------------------------
        
		} catch (UnsupportedEncodingException e) {
			logger.emitLog("ScanHtml - raised UnsupportedEncodingException: " + e.getMessage());
			throw new StorletException(e.getMessage());
		} catch (IOException e) {
			logger.emitLog(e.toString());
			logger.emitLog("ScanHtml - raised IOException: " + e.getMessage());
			throw new StorletException(e.getMessage());
		}
    }   
    
}

