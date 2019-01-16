package com.urv.vertigo.bus;

import java.io.IOException;
import com.urv.vertigo.bus.Backend.eLogLevel;

/*----------------------------------------------------------------------------
 * SBus
 * 
 * The front end Java class for Bus functionality.
 * */
public class Bus 
{
    private Handler hServerSideBus_;
    private Backend BusBack_;
    
    /*------------------------------------------------------------------------
     * CTOR
     * 
     * Instantiate the SBusBackend object. Start logging
     * */
    public Bus( final String contId ) throws IOException
    {
        BusBack_ = new Backend();
        BusBack_.startLogger( eLogLevel.BUS_LOG_DEBUG, contId );
    }

    /*------------------------------------------------------------------------
     * create
     * 
     * Initialize the server side SBus
     * */
    public void create( final String strPath ) throws IOException 
    {
        hServerSideBus_ = BusBack_.createBus( strPath );
    }

    /*------------------------------------------------------------------------
     * listen
     * 
     * Listen to the SBus. Suspend the executing thread
     * */
    public void listen() throws IOException 
    {
        BusBack_.listenBus(hServerSideBus_);
    }

    /*------------------------------------------------------------------------
     * receive
     * */
    public Datagram receive() throws IOException 
    {
        RawMessage Msg = BusBack_.receiveRawMessage( hServerSideBus_ );
        Datagram Dtg = new Datagram( Msg );
        return Dtg;
    }
    
    /*------------------------------------------------------------------------
     * send
     * */
    public void send( final String       strBusPath,
                      final Datagram Dtg         ) throws IOException 
    {
        
        RawMessage Msg = Dtg.toRawMessage();
        BusBack_.sendRawMessage(strBusPath, Msg);
    }

    /*------------------------------------------------------------------------
     * DTOR
     * 
     * Stop logging
     * */
    public void finalize()
    {
        BusBack_.stopLogger();
    }
}
