package com.ibm.storlet.sbus;

import java.io.IOException;
import com.ibm.storlet.sbus.SBusBackend.eLogLevel;

/*----------------------------------------------------------------------------
 * SBus
 * 
 * The front end Java class for SBus functionality.
 * */
public class SBus 
{
    private SBusHandler hServerSideSBus_;
    private SBusBackend SBusBack_;
    
    /*------------------------------------------------------------------------
     * CTOR
     * 
     * Instantiate the SBusBackend object. Start logging
     * */
    public SBus( final String contId ) throws IOException
    {
        SBusBack_ = new SBusBackend();
        SBusBack_.startLogger( eLogLevel.SBUS_LOG_DEBUG, contId );
    }

    /*------------------------------------------------------------------------
     * create
     * 
     * Initialize the server side SBus
     * */
    public void create( final String strPath ) throws IOException 
    {
        hServerSideSBus_ = SBusBack_.createSBus( strPath );
    }

    /*------------------------------------------------------------------------
     * listen
     * 
     * Listen to the SBus. Suspend the executing thread
     * */
    public void listen() throws IOException 
    {
        SBusBack_.listenSBus(hServerSideSBus_);
    }

    /*------------------------------------------------------------------------
     * receive
     * */
    public SBusDatagram receive() throws IOException 
    {
        SBusRawMessage Msg = SBusBack_.receiveRawMessage( hServerSideSBus_ );
        SBusDatagram Dtg = new SBusDatagram( Msg );
        return Dtg;
    }
    
    /*------------------------------------------------------------------------
     * send
     * */
    public void send( final String       strSBusPath,
                      final SBusDatagram Dtg         ) throws IOException 
    {
        
        SBusRawMessage Msg = Dtg.toRawMessage();
        SBusBack_.sendRawMessage(strSBusPath, Msg);
    }

    /*------------------------------------------------------------------------
     * DTOR
     * 
     * Stop logging
     * */
    public void finalize()
    {
        SBusBack_.stopLogger();
    }
}
