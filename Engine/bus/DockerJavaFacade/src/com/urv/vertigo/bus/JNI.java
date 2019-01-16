package com.urv.vertigo.bus;

/*----------------------------------------------------------------------------
 * JNI wrapper for low-level C API
 * 
 * Just declarations here.
 * See BusJNI.c for the implementation
 * */
public class JNI 
{
	static 
	{
		System.loadLibrary("jbus");
	}

	public native void startLogger(   final String         strLogLevel, final String contId );
	public native void stopLogger();
	public native int createBus(     final String         strBusName  );
	public native int listenBus(     int                  nBus        );
	public native int sendRawMessage( final String         strBusName,
                                      final RawMessage Msg         );
	public native RawMessage receiveRawMessage( int    nBus        );
}
