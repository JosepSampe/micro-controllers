package com.urv.storlet.transcoder;

import org.openstack.storlet.common.*;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.pdfbox.util.PDFTextStripper;


public class TranscoderStorlet implements IStorlet 
{
	private StorletLogger logger;
	
	/*------------------------------------------------------------------------
	 * getPDFMetadata
	 * */
	private Map<String, String> getPDFMetadata(PDDocumentInformation info) 
	{
		Map<String, String> metadata = new HashMap<String, String>();
		if (info.getTitle() != null) 
		{
    		metadata.put("title",info.getTitle());
    		logger.emitLog(info.getTitle());
		}
		if (info.getAuthor() != null) 
		{
    		metadata.put("author",info.getAuthor());
    		logger.emitLog(info.getAuthor());
		}
		if (info.getSubject() != null) 
		{
    		metadata.put("subject",info.getSubject());
    		logger.emitLog(info.getSubject());
		}
		if (info.getKeywords() != null) 
		{
    		metadata.put("keywords",info.getKeywords());
    		logger.emitLog(info.getKeywords());
		}
		if (info.getCreator() != null) 
		{
    		metadata.put("creator",info.getCreator());
    		logger.emitLog(info.getCreator());
		}
		if (info.getProducer() != null) 
		{
    		metadata.put("producer",info.getProducer());
    		logger.emitLog(info.getProducer());
		}
		return metadata;
	}

    /*------------------------------------------------------------------------
     * getPDFText
     * 
     * strPDFText and Metadata are assigned inside
     * */
	private String getPDFText( StorletInputStream  inStream,
	                           Map<String, String> Metadata )
	                                             throws StorletException
	{
	    String strPDFText = "";
	    try
	    {
    	    logger.emitLog("Consuming pdf content");
    	    PDFTextStripper pdfS = new PDFTextStripper();
    	    logger.emitLog("Going to read input md");
    	    HashMap<String, String> mdin = inStream.getMetadata();
    	    
    	    for (Map.Entry<String, String> entry : mdin.entrySet()) 
    	        logger.emitLog("Key = "     + entry.getKey() + 
    	                       ", Value = " + entry.getValue());
    	    logger.emitLog("Print md done");
    	    PDDocument pdoc = PDDocument.load(inStream.getStream());
    	    logger.emitLog("Get pdf Information");
    	    PDDocumentInformation info = pdoc.getDocumentInformation();
    	    logger.emitLog("Consuming pdf metadata");
    	    Map<String, String> md = getPDFMetadata(info);
    	    for( Map.Entry<String, String> mdi : md.entrySet() )
    	        Metadata.put( mdi.getKey(), mdi.getValue() );
    	    logger.emitLog("Transforming PDF");
    	    strPDFText =  pdfS.getText(pdoc);
    	    pdoc.close();
	    }
	    catch( IOException e )
	    {
            logger.emitLog(e.getMessage());
            throw new StorletException(e.getMessage());	        
	    }
	    return strPDFText;
	}
		
	/*------------------------------------------------------------------------
	 * Storlet invoke method.
	 * 
	 *  This storelt is used in various tests
	 *  1. GET/PUT, where the output stream is
	 *     of type StorletObjectOutputStream
	 *  2. PUT, where there are two output streams
	 *     - One of type StorletContainerOutputStream
	 *     - One of type StorletObjectOutputStream
	 *  In either case we end up with an OutputStream
	 *  which we write the text result to.
	 */
	@Override
	public void invoke(ArrayList<StorletInputStream>  arg0, 
	                   ArrayList<StorletOutputStream> arg1,
			           Map<String, String>            arg2, 
			           StorletLogger                  logger) 
			                                          throws StorletException 
    {
    	// CALCULATE TIME ------------
		long startTime, estimatedTime;
		startTime = System.nanoTime();
    	// ---------------------------
		
		try 
		{
			this.logger = logger;
			logger.emitLog("PDFtoText Storlet - Welcome");

			StorletObjectOutputStream	objectOutStream    = null;
			StorletContainerHandle		containerHandle = null;
			OutputStream				outputStream       = null;			
			String						strPDFText         = "";
			HashMap<String, String>  Metadata = new HashMap<String, String>();
			
			strPDFText = getPDFText( arg0.get(0), Metadata );			
	        for (StorletOutputStream outStream : arg1) 
	        {
	            if (outStream instanceof StorletObjectOutputStream) 
	            {
	                logger.emitLog("Got a StorletObjectOutputStream");
	                objectOutStream = (StorletObjectOutputStream)outStream;
	            }
	            else if (outStream instanceof StorletContainerHandle) 
	            {
	                logger.emitLog("Got a StorletContainerHandle");
	                containerHandle = (StorletContainerHandle)outStream;
	            }
	            else 
	                logger.emitLog("Unsupported type of output stream");
	        }       
			
    		if (objectOutStream != null) 
    		{
    			// Write metadata to object
    			logger.emitLog("About to set metadata");
    			objectOutStream.setMetadata(Metadata);
    			// Get the output stream where we write the final result
    			outputStream = objectOutStream.getStream();
    			logger.emitLog("Metadata is set");
    		} 
    		else 
    			logger.emitLog("No object out stream");
		
    		if (containerHandle != null) 
    		{
    			// Get an object stream from the container stream
    			logger.emitLog("container name is " + 
    			               containerHandle.getName());
    			logger.emitLog("Asking for output object");
    			StorletObjectOutputStream objStream = 
    			        containerHandle.getObjectOutputStream("Objname");
    			logger.emitLog("Got output object");
    			HashMap<String, String> md = new HashMap<String, String>();
    			md.put("key11", "value11");
    			md.put("key21", "value21");
    			logger.emitLog("About to set metadata");
    			objectOutStream.setMetadata(md);
    
    			// Get the output stream where we write the final result
    			logger.emitLog("About to write obj data");
    			OutputStream newOutputStream = objStream.getStream();
    			newOutputStream.write("New object content".getBytes());
    			newOutputStream.close();
    			logger.emitLog("Data written and stream closed");
    			
    		} 
    		else 
    			logger.emitLog("No container out stream");
		
    		if (outputStream != null) 
    		{
    			// Do the translation, and write the result to the stream.
    			outputStream.write(strPDFText.getBytes());
    			logger.emitLog("Done.");
    			outputStream.close();
    			
                // CALCULATE TIME ----------
                estimatedTime = System.nanoTime() - startTime;			
                logger.emitLog("Total Execution Time : " + estimatedTime); 
    			//--------------------------
    		} 
    		else 
    			logger.emitLog("No output stream");
		} 
		catch (IOException e) 
		{
				logger.emitLog(e.getMessage());
				throw new StorletException(e.getMessage());
		}
	}
}