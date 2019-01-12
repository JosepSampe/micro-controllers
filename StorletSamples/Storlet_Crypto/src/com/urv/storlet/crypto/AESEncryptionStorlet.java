package com.urv.storlet.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.openstack.storlet.common.*;

/**
 * 
 * @author Josep Sampe
 *
 */

public class AESEncryptionStorlet implements IStorlet {
	
	@Override
	public void invoke(ArrayList<StorletInputStream> inputStreams,
			ArrayList<StorletOutputStream> outputStreams,
			Map<String, String> parameters, StorletLogger log)
			throws StorletException {
		
		log.emitLog("CryptoStorlet Invoked");
		
		/*
		 * Prepare streams
		 */
		StorletInputStream sis = inputStreams.get(0);
		InputStream is = sis.getStream();
		HashMap<String, String> metadata = sis.getMetadata();
		
		StorletObjectOutputStream storletObjectOutputStream = (StorletObjectOutputStream)outputStreams.get(0);
		storletObjectOutputStream.setMetadata(metadata);
		OutputStream outputStream = storletObjectOutputStream.getStream();
		
		/*
		 * Initialize encryption engine
		 */
		String initVector = "RandomInitVector"; // 16 bytes IV
		IvParameterSpec iv = null;
		SecretKeySpec skeySpec = null;
		Cipher cipher = null;
		String key = "getActualKeyFrom"; //TODO: Get keys from Redis, 16 bytes key!
		try {
			iv = new IvParameterSpec(initVector.getBytes("UTF-8"));
			skeySpec = new SecretKeySpec(key.getBytes("UTF-8"), "AES");
	        cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");   
			/*
			 * Get reverse flag to decide if to encrypt/decrypt
			 */
	        //TODO: Check that the  parameter usage is correct
			if (parameters.get("reverse") != null && Boolean.parseBoolean(parameters.get("reverse"))) {
				cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
			}else cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);

			byte[] buffer = new byte[1024];
			int len = 0;
			while((len = is.read(buffer)) != -1) {
				byte[] cryptoContent = cipher.update(buffer);
				outputStream.write(cryptoContent, 0, cryptoContent.length);
				//TODO: As AES does padding, we may need to store the original object len on encryption
			}
		} catch (IOException e) {
			log.emitLog("Encryption - raised IOException: " + e.getMessage());
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (InvalidAlgorithmParameterException e) {
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			e.printStackTrace();
		} finally {
			try {
				is.close();
				outputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		log.emitLog("EncryptionStorlet Invocation done");
	}
}
