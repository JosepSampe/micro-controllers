package com.urv.storlet.watermark;

import org.openstack.storlet.common.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;


public class WatermarkStorlet implements IStorlet {

	private static final String DEFAULT_TYPE = "webm";
	private static final String DEFAULT_TEXT = "Watermark-Josep";
    // Get the path of this class image
    private final String strJarPath = StorletUtils.getClassFolder(this.getClass());

    @Override
	public void invoke(ArrayList<StorletInputStream> inStreams,
			ArrayList<StorletOutputStream> outStreams,
			Map<String, String> parameters,
			final StorletLogger logger) throws StorletException {

		final StorletInputStream sis = inStreams.get(0);
		final InputStream is = sis.getStream();
		HashMap<String, String> metadata = sis.getMetadata();

		final StorletObjectOutputStream sos = (StorletObjectOutputStream)outStreams.get(0);
		final OutputStream os = sos.getStream();
		
		// Copy metadata and write to stolret output asap to avoid timeouts
		//sos.setMetadata(metadata);
		
        try {
        	logger.emitLog(new Date().toString() + " --- " + this.getClass().getName() + " - Start");
            String type = parameters.get("type");
            if (type == null || type.isEmpty()) {
                type = DEFAULT_TYPE;
            }
            
            String text = parameters.get("text");
            if (text == null || text.isEmpty()) {
                text = DEFAULT_TEXT;
            }
            
    		// Copy metadata and write to stolret output asap to avoid timeouts
    		sos.setMetadata(metadata);
    		
            // Run ImageMagick
            Runtime runtime = Runtime.getRuntime();
            Process p = null;

            
            try {
            	//String command = "/usr/bin/convert -fill \"rgba(80%%,80%%,80%%,0.70)\" -background \"#00000000\" -pointsize 36 -gravity center label:\"Josep\" /tmp/label_centered.png";
            	String[] command = new String[] {"/usr/bin/convert", "-fill", "rgba(80%%,80%%,80%%,0.70)", "-background", "#00000000", "-pointsize", "36", "-gravity", "center", "label:" + text, "/tmp/label_centered.png"};
            	//logger.emitLog(command);
            	p = runtime.exec(command);
            
            } catch (IOException ex) {
            	logger.emitLog(Arrays.toString(ex.getStackTrace()));
            }
            int exitCode = p.waitFor();
            logger.emitLog("imageMagick returned with code " + exitCode);
            
            // Run ffmpeg
            String ffmpeg = strJarPath + File.separatorChar + "ffmpeg";
            String[] command = new String[] {ffmpeg, "-f", type, "-i", "-", "-vf", "movie=/tmp/label_centered.png [watermark]; [in][watermark] overlay=50:50 [out]", "-loglevel", "panic", "-f", "webm", "-"};
            runtime = Runtime.getRuntime();
            p = null;
            try {
                p = runtime.exec(command);
            } catch (IOException ex) {
            	logger.emitLog(Arrays.toString(ex.getStackTrace()));
            }

            final OutputStream pOutputStream = p.getOutputStream();
            final InputStream pInputStream = p.getInputStream();

            // Read the input stream piping it to ffmpeg
            Thread storletInput2ffmpeg = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        IOUtils.copyLarge(is, pOutputStream);
                        pOutputStream.flush();
                        pOutputStream.close();
                    } catch (IOException ex) {
                    	logger.emitLog(Arrays.toString(ex.getStackTrace()));
                    }
                }
            });

            // Read ffmpeg output messages from input stream
            Thread ffmpegInputStream = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        IOUtils.copyLarge(pInputStream, os);
                        os.flush();
                        os.close();
                    } catch (IOException ex) {
                    	logger.emitLog(Arrays.toString(ex.getStackTrace()));
                    }
                }
            });

            storletInput2ffmpeg.start();
            ffmpegInputStream.start();
            exitCode = p.waitFor();
            storletInput2ffmpeg.join();
            ffmpegInputStream.join();

            pInputStream.close();
            pOutputStream.close();

        } catch (IOException | InterruptedException | StorletException ex) {
        //} catch (IOException | InterruptedException ex) {
            logger.emitLog(Arrays.toString(ex.getStackTrace()));
            throw new StorletException(Arrays.toString(ex.getStackTrace()));
        } finally {
            // Close the streams
            try {
                if (is != null) {
                    is.close();
                }
                if (os != null) {
                    os.close();
                }
            } catch (IOException ex) {
                logger.emitLog(Arrays.toString(ex.getStackTrace()));
            }
        }
    }
}
