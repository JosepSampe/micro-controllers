package com.urv.storlet.adult;

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


public class AdultStorlet implements IStorlet {
	
	private  HashMap<String, Integer> header_;

    public AdultStorlet()
    {
        header_ = new HashMap<>();
        loadHeader();
    }

    private void loadHeader()
    {
        String header[] = {
            "ssn", "age", "workclass", "fnlwgt", "education", "education-num", "marital-status", "occupation", "relationship", "race", "sex", "capital-gain", "capital-loss", "hours-per-week", "native-country", "prediction"
        };
        
        for(int i = 0; i < header.length; i++)
        	header_.put(header[i], i);

    }

    public void invoke(ArrayList<StorletInputStream> inStreams, ArrayList<StorletOutputStream> outStreams, 
    				  Map<String, String> parameters, StorletLogger logger) throws StorletException {
        
    	
    	// CALCULATE TIME ------------
		long startTime, estimatedTime;
		startTime = System.nanoTime();
    	// ---------------------------
		
    	//logger.emitLog("Wellcome to Ubuntu One Trace storlet");
        logger.emitLog("Going to read input stream");
        StorletInputStream sis = inStreams.get(0);
		InputStream inputStream = sis.getStream();
		//logger.emitLog("-> Done.");
		HashMap<String, String> metadata = sis.getMetadata();
        
        //GET PARAMETERS
		logger.emitLog("Going to read parameters");
        String select_string = parameters.get("select");
        String where_string = parameters.get("where");
        //logger.emitLog("-> Done.");
        
        //PUT METADATA
        logger.emitLog("Adding projection and selection fields to the metadata");
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
	        logger.emitLog("Parsing where parameters");
	        where = where_string.replaceAll("\\s+", "").split(" and ");
        }
               
        // READ OUTPUT STREAM
        logger.emitLog("Going to read output Stream");
		StorletObjectOutputStream storletObjectOutputStream = (StorletObjectOutputStream)outStreams.get(0);
		storletObjectOutputStream.setMetadata(metadata);
		OutputStream outputStream = storletObjectOutputStream.getStream();

		logger.emitLog(select_string);	
				
        try {


        	BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
            logger.emitLog("Going to read Input File");
            
            String line = null;
            String trace_line[];
            String where_condition[];
            String out_line = "";
            boolean valid;
            int k,l;
            
            //long aa, bb;
            //long total = 0;
            //int count= 0;

			int bufferLength = 65536;
			ByteBuffer bbuf = ByteBuffer.allocate(bufferLength);
            
            while ((line = br.readLine()) != null) {
            	//count++;
            	valid = true;
            	
            	//aa = System.nanoTime();
            	
                trace_line = line.split(","); // converts line to vector // ESTA LIMITANDO EL TRHOUGHPUT
            	                
                
                if (trace_line.length < header_.size()) continue;
            	
                if ( where_string != null ){ //TODO: IMPLEMENT OR CLAUSE
	                // CHECK WHERE CLAUSE
	                for(k = 0; k < where.length; k++){
	                    where_condition = where[k].split("=");
	                    if(!trace_line[header_.get(where_condition[0])].equals(where_condition[1])){ // IF LINE DON'T MATCH WITH WHERE CLAUSE -> LINE NOT VALID
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
	                    bbuf.put(out_line.getBytes());
	                }
                	
	            	if (bbuf.remaining() < 100){
	            		outputStream.write(bbuf.array());
	            		bbuf.clear();
	            	}    	
                }
                

            	//bb = System.nanoTime() - aa;
            	//total = total + bb;
            	
            	//outputStream.write(line.getBytes());

            }
            
            //logger.emitLog("--> Mean:"+String.valueOf(total/count));
            
	        br.close();
	        outputStream.close();
	        inputStream.close();
	        
            // CALCULATE TIME ----------
            estimatedTime = System.nanoTime() - startTime;			
            logger.emitLog("Total Execution Time : " + estimatedTime); 
			//--------------------------
        
		} catch (UnsupportedEncodingException e) {
			logger.emitLog("Adult - raised UnsupportedEncodingException: " + e.getMessage());
			throw new StorletException(e.getMessage());
		} catch (IOException e) {
			logger.emitLog(e.toString());
			logger.emitLog("Adult - raised IOException: " + e.getMessage());
			throw new StorletException(e.getMessage());
		}
    }   
    
}

