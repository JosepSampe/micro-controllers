package com.urv.storlet.blurfaces;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.*;
import java.io.File;

import org.apache.commons.compress.utils.IOUtils;

import org.openstack.storlet.common.*;


public class BlurFacesStorlet implements IStorlet {
	/*
	 public long comp_a, total, comp_b;
	 public long io_b;
	public long io_a;
	public long totalio;
	public  long io=0;*/
	
	/***
	 * Storlet invoke method. 
	 */
	@Override
	public void invoke(ArrayList<StorletInputStream> inStreams,
			ArrayList<StorletOutputStream> outStreams,
			Map<String, String> parameters,
			StorletLogger logger) throws StorletException {
				
		//this.comp_b = System.nanoTime();
		
		InputStream is = inStreams.get(0).getStream();

		HashMap<String, String> metadata = inStreams.get(0).getMetadata();
		
		StorletObjectOutputStream storletObjectOutputStream = (StorletObjectOutputStream)outStreams.get(0);
		OutputStream os = storletObjectOutputStream.getStream();
		
		String blurring_level = "50";
		if (parameters.get("blur") != null) {
			blurring_level = parameters.get("blur"); 
		}
		
		logger.emitLog("-> Init OpenCV.");
		initializeOpenCV(is);
		
		logger.emitLog("-> Init BlurFaces.");
		blurFaces(blurring_level, metadata);
		
		((StorletObjectOutputStream) outStreams.get(0) ).setMetadata(metadata);
		logger.emitLog("-> Init ReturnImage.");
		returnBlurredImage(os);
		
		
		//this.comp_a = System.nanoTime();				
		//this.total = this.comp_a-this.comp_b;	
				
		//logger.emitLog("IO [ms]: "+((this.io)/1000000L));
		//logger.emitLog("CPU [ms]: "+((this.comp_a-this.io)/1000000L));
		
		try {
			is.close();
			os.close();
		} catch (IOException e) {
			throw new StorletException(e.getMessage());
		}
		
	}

	private void returnBlurredImage(OutputStream os) throws StorletException {
		try {
	        // copy file to output stream
	        InputStream blurredImage = new FileInputStream(new File("/tmp/output/output.jpg"));
	        IOUtils.copy(blurredImage, os);
	        
		} catch (IOException e) {
			throw new StorletException(e.getMessage());
		}
	}

	private void blurFaces(String blurring_level, HashMap<String, String> metadata) throws StorletException {
                //if (2<3)
                //   throw new StorletException("GLLLL");
		try {
			Process p;
			
			String num_rect = metadata.get("Num-Rect-I"); 
			if (num_rect != null) {
				PrintWriter writer = new PrintWriter("/tmp/input/metadata");
				writer.println(num_rect);
				
				int num_rect_int = Integer.parseInt(num_rect);
				
				for (int i=0; i < num_rect_int; i++) {
					 writer.println(metadata.get("Rect" + i));
				}
				writer.close();
				
				// run blurring script
				//p = Runtime.getRuntime().exec("/tmp/forgetit/blurfaces/run_blur.sh.moo " + blurring_level);
				p = Runtime.getRuntime().exec("/tmp/forgetit/blurfaces/run_blur.sh " + blurring_level);
				p.waitFor();
			} else {
				// run blurring script
				//p = Runtime.getRuntime().exec("/tmp/forgetit/blurfaces/run_recognize_blur.sh.moo " + blurring_level);
				p = Runtime.getRuntime().exec("/tmp/forgetit/blurfaces/run_recognize_blur.sh " + blurring_level);
				p.waitFor();
			}
		} catch (IOException | InterruptedException e) {
			throw new StorletException(e.getMessage());
		}
	}

	private void initializeOpenCV(InputStream is) throws StorletException {
		try {
			Process p;
			
			File f = new File("/tmp/forgetit/blurfaces");
			if (!(f.exists())) {
				p = Runtime.getRuntime().exec("mkdir -p /tmp/forgetit/blurfaces");
	            p.waitFor();
	            p = Runtime.getRuntime().exec("tar xvfz /home/swift/com.ibm.storlet.blurfaces.BlurFacesStorlet/blur_faces_all.tar.gz -C /tmp/forgetit/blurfaces");
	            p.waitFor();
	        }
			
			p = Runtime.getRuntime().exec("rm -Rf /tmp/input /tmp/output");
			p.waitFor();
			
	
			p = Runtime.getRuntime().exec("mkdir /tmp/input /tmp/output");
			p.waitFor();
			
			// copy input stream to file
			OutputStream objectFile = new FileOutputStream(new File("/tmp/input/object.jpg"));
			
			//this.io_b = System.nanoTime();
			IOUtils.copy(is, objectFile);
			//this.io_a = System.nanoTime();				
			//this.totalio = this.io_a-this.io_b;	
			//this.io=this.io+this.totalio;
			
		} catch (IOException | InterruptedException e) {
			throw new StorletException(e.getMessage());
		}
	}
}
