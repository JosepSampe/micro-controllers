package com.urv.storlet.compress;

import com.ibm.storlet.common.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import org.apache.commons.compress.utils.IOUtils;

/**
 * 
 * @author Josep Sampe
 *
 */

public class CompressStorlet implements IStorlet {
	
	private static final String DEFAULT_COMPRESSION = "lz4";
	
	/***
	 * Storlet invoke method. 
	 */
	@Override
	public void invoke(ArrayList<StorletInputStream> inStreams,
			ArrayList<StorletOutputStream> outStreams,
			Map<String, String> parameters,
			StorletLogger logger) throws StorletException {
		
		logger.emitLog("Init Storlet");
		StorletInputStream sis = inStreams.get(0);
		InputStream is = sis.getStream();
		HashMap<String, String> metadata = sis.getMetadata();

		StorletObjectOutputStream sos = (StorletObjectOutputStream)outStreams.get(0);
		OutputStream os = sos.getStream();
		sos.setMetadata(metadata);
		
		
		String type = parameters.get("compression");
		
		if (type == null || type.isEmpty()){
			type = DEFAULT_COMPRESSION;
		}
		
		try {	
			OutputStream compress;
			
			if (type.equals("gz")){
				
				long before = System.nanoTime();
				compress = new GZIPOutputStream(os);
				IOUtils.copy(is, compress);
				compress.close();
				long after = System.nanoTime();
				logger.emitLog("GZIP--Elapsed [ms]: "+((after-before)/1000000L));
			
			} else if (type.equals("zip")){ 
				
				long before = System.nanoTime();
				String source="file"; //file_name
				ZipEntry ze= new ZipEntry(source);
				compress = new ZipOutputStream(os);
				((ZipOutputStream) compress).putNextEntry(ze);
		 
		        byte[] buf = new byte[1024];
		        int len;
		        while((len=is.read(buf))>0){
		        	compress.write(buf, 0, len);
		        }
		        compress.close();
		        long after = System.nanoTime();
				logger.emitLog("ZIP--Elapsed [ms]: "+((after-before)/1000000L));
		        
			} else { 
		      
				int len = 0, size;
				int bufferLength = 65536;
				byte[] buffer = new byte[bufferLength];
				
				LZ4Factory factory = LZ4Factory.safeInstance();
				LZ4Compressor compressor = factory.fastCompressor();
				int maxCompressedLength = compressor.maxCompressedLength(bufferLength);
				byte[] compressed = new byte[maxCompressedLength];
				long before = System.nanoTime();
				
				
				//long comp_a, total, comp_b, comp=0;
				//long io_b, io_a, totalio, io=0;
				
				
				while ((len = is.read(buffer)) > 0) {
					//comp_b = System.nanoTime();
					size = compressor.compress(buffer, 0, len, compressed, 0, maxCompressedLength);
					//comp_a = System.nanoTime();				
					//total = comp_a-comp_b;	
					//comp=comp+total;
					
					//io_b = System.nanoTime();
					os.write(buffer, 0, size);
					//io_a = System.nanoTime();				
					//totalio = io_a-io_b;	
					//io=io+totalio;
				}

				long after = System.nanoTime();
				logger.emitLog("ZL4--Elapsed [ms]: "+((after-before)/1000000L));
				//logger.emitLog("IO [ms]: "+((io)/1000000L));
				//logger.emitLog("CPU [ms]: "+((comp)/1000000L));
			}

			is.close();
			os.close();	
		} catch (UnsupportedEncodingException e) {
			logger.emitLog("Compress Storlet - raised UnsupportedEncodingException: " + e.getMessage());
			throw new StorletException(e.getMessage());
		} catch (IOException e) {
			logger.emitLog("Compress Storlet - raised IOException: " + e.getMessage());
			throw new StorletException(e.getMessage());
		}
	}
}