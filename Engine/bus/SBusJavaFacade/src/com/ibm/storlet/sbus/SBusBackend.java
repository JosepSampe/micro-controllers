package com.ibm.storlet.sbus;

import java.io.IOException;

/*----------------------------------------------------------------------------
 * SBusBackend
 * 
 * This class wraps and transfers calls to the JNI implementation 
 * */
public class SBusBackend 
{
	/*------------------------------------------------------------------------
	 * JNI layer delegate, common to every instance of SBusBackend
	 * */
	private static SBusJNI SBusJNIObj_  = new SBusJNI();
	
	/*------------------------------------------------------------------------
	 * Enumerating logging levels
	 * The values are suitable to syslog constants
	 * */
	public static enum eLogLevel
	{
		SBUS_LOG_DEBUG,
		SBUS_LOG_INFO,
		SBUS_LOG_WARNING,
		SBUS_LOG_CRITICAL,
		SBUS_LOG_OFF
	};
		
	/*------------------------------------------------------------------------
	 * Initiate logging with the required detail level 
	 * */
	public void startLogger( eLogLevel eLogLevel, String contId )
	{
		String strLogLevel = null;
		switch( eLogLevel )
		{
		case SBUS_LOG_DEBUG:
			strLogLevel = "DEBUG";
			break;
		case SBUS_LOG_INFO:
			strLogLevel = "INFO";
			break;
		case SBUS_LOG_WARNING:
			strLogLevel = "WARNING";
			break;
		case SBUS_LOG_CRITICAL:
			strLogLevel = "CRITICAL";
			break;
		case SBUS_LOG_OFF:
			strLogLevel = "OFF";
			break;
		default:
			strLogLevel = "WARNINIG";
			break;
		}
		SBusJNIObj_.startLogger(strLogLevel, contId);
	}
	
	/*------------------------------------------------------------------------
	 * Stop logging 
	 * */
	public void stopLogger()
	{
		SBusJNIObj_.stopLogger();
	}
	
	/*------------------------------------------------------------------------
	 * Create the bus. 
	 * */
	public SBusHandler createSBus( final String strSBusName ) 
			                                                throws IOException
	{
		int nSBus = SBusJNIObj_.createSBus( strSBusName );
		if( 0 > nSBus )
			throw new IOException( "Unable to create SBus - " + strSBusName );
		return new SBusHandler( nSBus );
	}
	
	/*------------------------------------------------------------------------
	 * Wait and listen to the bus.
	 * The executing thread is suspended until some data arrives. 
	 * */
	public boolean listenSBus( final SBusHandler hSBus ) 
			                                                throws IOException
	{
		int nStatus = SBusJNIObj_.listenSBus( hSBus.getFD() );
		if( 0 > nStatus )
			throw new IOException( "Unable to listen to SBus" );
		return true;
	}
	
	/*------------------------------------------------------------------------
	 * Take the message and send it.
	 * */
	public int sendRawMessage( final String 		strBusName, 
			                   final SBusRawMessage Msg ) 
			                		                       throws IOException
	{
		int nStatus = SBusJNIObj_.sendRawMessage(strBusName, Msg );
		if( 0 > nStatus )
			throw new IOException( "Unable to send message" );
		return nStatus;
	}
	
	/*------------------------------------------------------------------------
	 * Read some actual raw data from the bus
	 * */
	public SBusRawMessage receiveRawMessage( final SBusHandler hSBus )
	                                                        throws IOException
	{
		SBusRawMessage Msg = SBusJNIObj_.receiveRawMessage( hSBus.getFD() );
		if( null == Msg )
			throw new IOException( "Unable to retrieve a message" );
		return Msg;
	}
	
}
