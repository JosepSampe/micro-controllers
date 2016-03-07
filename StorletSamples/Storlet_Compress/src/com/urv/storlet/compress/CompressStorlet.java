package com.urv.storlet.compress;

import com.ibm.storlet.common.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * 
 * @author Josep Sampe
 *
 */

public class CompressStorlet implements IStorlet {
	
	private static final String DEFAULT_COMPRESSION = "gz";
	
	/***
	 * Storlet invoke method. 
	 */
	@Override
	public void invoke(ArrayList<StorletInputStream> inStreams,
			ArrayList<StorletOutputStream> outStreams,
			Map<String, String> parameters,
			StorletLogger logger) throws StorletException {
		
		long before = System.nanoTime();
		logger.emitLog("----- Init Compression Storlet -----");
		StorletInputStream sis = inStreams.get(0);
		InputStream is = sis.getStream();
		HashMap<String, String> metadata = sis.getMetadata();

		StorletObjectOutputStream sos = (StorletObjectOutputStream)outStreams.get(0);
		OutputStream os = sos.getStream();
		sos.setMetadata(metadata);
		
		String type = parameters.get("compression");
		String reverse = parameters.get("reverse");
		
		if (type == null || type.isEmpty()){
			type = DEFAULT_COMPRESSION;
		}
		if (reverse == null || reverse.isEmpty()){
			reverse = "False";
		}
		
		byte[] buffer = new byte[65536];
		int len;
		
		try {				
			if (type.equals("gz")){

				if (reverse.equals("True")){
					logger.emitLog("GZIP Decompression");
					GZIPInputStream compressed = new GZIPInputStream(is);
					while((len = compressed.read(buffer)) != -1) {
						os.write(buffer, 0, len);
					}
					compressed.close();
				} else {
					
					GZIPOutputStream tocompress = new GZIPOutputStream(os);
					while((len=is.read(buffer)) != -1) {
						tocompress.write(buffer, 0, len);
					}
					tocompress.close();
				}
							
			} else if (type.equals("zip")){

				if (reverse.equals("True")){
					logger.emitLog("ZIP Decompression");
					ZipInputStream compressed = new ZipInputStream(is); 
					ZipEntry entry = (ZipEntry) compressed.getNextEntry();
			        while((len=compressed.read(buffer))>0){
			        	os.write(buffer, 0, len);
			        }
			        compressed.close();
				} else {
					logger.emitLog("ZIP Compression");
					ZipOutputStream compress = new ZipOutputStream(os);
					compress.putNextEntry(new ZipEntry("data"));
			        while((len=is.read(buffer))>0){
			        	compress.write(buffer, 0, len);
			        }
			        compress.close();
				}   
			} 

			is.close();
			os.close();
			
			long after = System.nanoTime();
			logger.emitLog("Compression Storlet -- Elapsed [ms]: "+((after-before)/1000000L));
		} catch (IOException e) {
			logger.emitLog("Compress Storlet - raised IOException: " + e.getMessage());
		}
	}
}