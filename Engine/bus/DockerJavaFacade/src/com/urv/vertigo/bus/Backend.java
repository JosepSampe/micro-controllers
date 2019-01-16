package com.urv.vertigo.bus;

import java.io.IOException;

/*----------------------------------------------------------------------------
 * Backend
 * 
 * This class wraps and transfers calls to the JNI implementation 
 * */
public class Backend 
{
	/*------------------------------------------------------------------------
	 * JNI layer delegate, common to every instance of SBusBackend
	 * */
	private static JNI BusJNIObj_  = new JNI();
	
	/*------------------------------------------------------------------------
	 * Enumerating logging levels
	 * The values are suitable to syslog constants
	 * */
	public static enum eLogLevel
	{
		BUS_LOG_DEBUG,
		BUS_LOG_INFO,
		BUS_LOG_WARNING,
		BUS_LOG_CRITICAL,
		BUS_LOG_OFF
	};
		
	/*------------------------------------------------------------------------
	 * Initiate logging with the required detail level 
	 * */
	public void startLogger( eLogLevel eLogLevel, String contId )
	{
		String strLogLevel = null;
		switch( eLogLevel )
		{
		case BUS_LOG_DEBUG:
			strLogLevel = "DEBUG";
			break;
		case BUS_LOG_INFO:
			strLogLevel = "INFO";
			break;
		case BUS_LOG_WARNING:
			strLogLevel = "WARNING";
			break;
		case BUS_LOG_CRITICAL:
			strLogLevel = "CRITICAL";
			break;
		case BUS_LOG_OFF:
			strLogLevel = "OFF";
			break;
		default:
			strLogLevel = "WARNINIG";
			break;
		}
		BusJNIObj_.startLogger(strLogLevel, contId);
	}
	
	/*------------------------------------------------------------------------
	 * Stop logging 
	 * */
	public void stopLogger()
	{
		BusJNIObj_.stopLogger();
	}
	
	/*------------------------------------------------------------------------
	 * Create the bus. 
	 * */
	public Handler createBus( final String strBusName ) 
			                                                throws IOException
	{
		int nSBus = BusJNIObj_.createBus( strBusName );
		if( 0 > nSBus )
			throw new IOException( "Unable to create SBus - " + strBusName );
		return new Handler( nSBus );
	}
	
	/*------------------------------------------------------------------------
	 * Wait and listen to the bus.
	 * The executing thread is suspended until some data arrives. 
	 * */
	public boolean listenBus( final Handler hBus ) 
			                                                throws IOException
	{
		int nStatus = BusJNIObj_.listenBus( hBus.getFD() );
		if( 0 > nStatus )
			throw new IOException( "Unable to listen to SBus" );
		return true;
	}
	
	/*------------------------------------------------------------------------
	 * Take the message and send it.
	 * */
	public int sendRawMessage( final String 		strBusName, 
			                   final RawMessage Msg ) 
			                		                       throws IOException
	{
		int nStatus = BusJNIObj_.sendRawMessage(strBusName, Msg );
		if( 0 > nStatus )
			throw new IOException( "Unable to send message" );
		return nStatus;
	}
	
	/*------------------------------------------------------------------------
	 * Read some actual raw data from the bus
	 * */
	public RawMessage receiveRawMessage( final Handler hBus )
	                                                        throws IOException
	{
		RawMessage Msg = BusJNIObj_.receiveRawMessage( hBus.getFD() );
		if( null == Msg )
			throw new IOException( "Unable to retrieve a message" );
		return Msg;
	}
	
}
