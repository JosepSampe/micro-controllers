package com.ibm.storlet.sbus;

/*----------------------------------------------------------------------------
 * JNI wrapper for low-level C API
 * 
 * Just declarations here.
 * See SBusJNI.c for the implementation
 * */
public class SBusJNI 
{
	static 
	{
		System.loadLibrary("jsbus");
	}

	public native void startLogger(   final String         strLogLevel, final String contId );
	public native void stopLogger();
	public native int createSBus(     final String         strBusName  );
	public native int listenSBus(     int                  nBus        );
	public native int sendRawMessage( final String         strBusName,
                                      final SBusRawMessage Msg         );
	public native SBusRawMessage receiveRawMessage( int    nBus        );
}
