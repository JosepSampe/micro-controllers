package com.urv.storlet.uonetrace;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.ibm.storlet.common.*;


/**
 * 
 * @author Josep Sampe
 *
 */


public class UOneTraceStorlet implements IStorlet {
	
	private  HashMap<String, Integer> header_;

    public UOneTraceStorlet()
    {
        header_ = new HashMap<>();
        loadHeader();
    }

    private void loadHeader()
    {
    	
    	// Ad-hoc headers: only works with ubuntu-one trace.
        String header[] = {
            "t", "addr", "caps", "client_metadata", "current_gen", "ext", "failed", "free_bytes", "from_gen", "hash", 
            "level", "logfile_id", "method", "mime", "msg", "node_id", "nodes", "pid", "req_id", "req_t", 
            "root", "server", "shared_by", "shared_to", "shares", "sid", "size", "time", "tstamp", "type", 
            "udfs", "user_id", "user", "vol_id"
        };
        for(int i = 0; i < header.length; i++)
        	header_.put(header[i], i);

    }

	/***
	 * Storlet invoke method
	 */
    public void invoke(ArrayList<StorletInputStream> inStreams, ArrayList<StorletOutputStream> outStreams, 
    				  Map<String, String> parameters, StorletLogger logger) throws StorletException {
        
    	//logger.emitLog("Wellcome to Ubuntu One Trace storlet");
        //logger.emitLog("Going to read input stream");
        StorletInputStream sis = inStreams.get(0);
		InputStream inputStream = sis.getStream();
		//logger.emitLog("-> Done.");
		HashMap<String, String> metadata = sis.getMetadata();
        
        //GET PARAMETERS
		//logger.emitLog("Going to read parameters");
        String select_string = parameters.get("select");
        String where_string = parameters.get("where");
        //logger.emitLog("-> Done.");
        
        //PUT METADATA
        //logger.emitLog("Adding projection and selection fields to the metadata");
        metadata.put("select", select_string);
        metadata.put("where", where_string);

        Iterator<String> keySetIterator = metadata.keySet().iterator(); 
       
        while(keySetIterator.hasNext()){ 
        	String key = keySetIterator.next();
        	logger.emitLog("key: " + key + " value: " + metadata.get(key));
        }
                
        String select[] = null;
        String where[] = null;
        
        // PARSING SELECT PARAMETERS
        if (select_string != null){
	        //logger.emitLog("Parsing select parameters");
	        select = select_string.replaceAll("\\s+", "").split(",");
        }
        
        // PARSING WHERE PARAMETERS
        if (where_string != null){
	        //logger.emitLog("Parsing where parameters");
	        where = where_string.replaceAll("\\s+", "").split(" and ");
        }
               
        // READ OUTPUT STREAM
        //logger.emitLog("Going to read output Stream");
		StorletObjectOutputStream storletObjectOutputStream = (StorletObjectOutputStream)outStreams.get(0);
		storletObjectOutputStream.setMetadata(metadata);
		OutputStream outputStream = storletObjectOutputStream.getStream();
				
        try {

        	BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
            logger.emitLog("Going to read Input File");
            
            String s, line, out_line;
            String trace_line[], where_condition[];
            boolean valid;
            int k,l;
            
			int bufferLength = 65536;
			ByteBuffer bbuf = ByteBuffer.allocate(bufferLength);
            
            while ((line = br.readLine()) != null) {
            	valid = true;

                // SEARCH FROZENSET (Only for ubuntu-one trace)               
        		if (line.indexOf("frozenset")>-1){
        			s = line.substring(line.indexOf("\"frozenset") + 1);
        			s = s.substring(0, s.indexOf("\""));
        			line = line.replace(s, "");
        		}
                
        		// convert line to vector
                trace_line = line.split(",");
                // After measure the performance of the Storlet I found that split function limits its performance.
                // Split() is the most expensive operation in each iteration, maybe other technique (String tokenizer 
                // or other) is better here.
                
                //logger.emitLog(Integer.toString(trace_line.length));
                
                if (trace_line.length < header_.size()) continue;

                if ( where_string != null ){ //TODO: IMPLEMENT OR CLAUSE
	                // Check where clause
	                for(k = 0; k < where.length; k++){
	                    where_condition = where[k].split("=");
	                    // IF LINE DON'T MATCH WITH WHERE CLAUSE -> LINE NOT VALID
	                    if(!trace_line[header_.get(where_condition[0])].equals(where_condition[1])){
	                    	valid = false;
	                        break;
	                    }
	                }
                }

                // FINALLY IF LINE IS VALID -> RETURN IT
                out_line = "";
            	if(valid){
                	if ( select_string == null || select[0].equals("*")){
	                	outputStream.write((line+"\n").getBytes());
	                } else {                	
	                    for(l = 0; l < select.length; l++){
	                    	if (l==0){
	                    		out_line = trace_line[header_.get(select[l])];
	                    	} else {
	                    		out_line = out_line+","+trace_line[header_.get(select[l])];
	                    	}
	                    }
	                    out_line = out_line+"\n";
	                    // Push line into the buffer:  Analyzing the performance of the Storlet I found that
	                	// send the data line by line with "outputStream.write(out_line.getBytes())" is an
	                	// expensive operation, so is better (and overall fastest) save the results in an
	                    // intermediate buffer.
	                    bbuf.put(out_line.getBytes());
	                }
                	
                	// We only return the data when the buffer is almost full.
	            	if (bbuf.remaining() < 100){
	            		outputStream.write(bbuf.array());
	            		bbuf.clear();
	            	}    	
                }
            }

			
	        br.close();
	        outputStream.close();
	        inputStream.close();
        
		} catch (UnsupportedEncodingException e) {
			logger.emitLog("UOneTrace - raised UnsupportedEncodingException: " + e.getMessage());
			throw new StorletException(e.getMessage());
		} catch (IOException e) {
			logger.emitLog(e.toString());
			logger.emitLog("UOneTrace - raised IOException: " + e.getMessage());
			throw new StorletException(e.getMessage());
		}
    }
}

