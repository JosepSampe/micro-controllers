package com.urv.storlet.transcoder_;

import java.io.IOException;
import java.util.*;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

import org.openstack.storlet.common.*;


public class TranscoderStorlet_ implements IStorlet {

	
	/***
	 * Storlet invoke method. 
	 */
	@Override
	public void invoke(ArrayList<StorletInputStream>  arg0, 
	                   ArrayList<StorletOutputStream> arg1,
			           Map<String, String>            arg2, 
			           StorletLogger                  logger) 
			                                          throws StorletException 
    {
		try
		{
    		logger.emitLog("PDFtoText Storlet - Welcome");
    		PDFTextStripper pdfS = new PDFTextStripper();
    		PDDocument pdoc = PDDocument.load(arg0.get(0).getStream());
    		logger.emitLog("Going to transform PDF");
    		String pdText = pdfS.getText(pdoc);
    		logger.emitLog("Done.");
    	
            HashMap<String, String> mdin = arg0.get(0).getMetadata();    		
    		StorletObjectOutputStream outStream = (StorletObjectOutputStream)
    		                                      arg1.get(0);
    		outStream.setMetadata( mdin );
    		outStream.getStream().write(pdText.getBytes());
    		pdoc.close();
    		outStream.getStream().close();

		} 
		catch (IOException e) 
		{
			logger.emitLog(e.getMessage());
			throw new StorletException(e.getMessage());
		}
    }
}
