package com.urv.vertigo.bus;

/*----------------------------------------------------------------------------
 * This class encapsulates OS level file descriptor used 
 * in Transport Layer APIs. 
 * */

public class Handler 
{
	private int nFD_;

	/*------------------------------------------------------------------------
	 * CTOR
	 * No default value
	 * */
	public Handler( int nFD )
	{
		nFD_ = nFD;
	}

	/*------------------------------------------------------------------------
	 * Getter
	 * */
	public int getFD()
	{
		return nFD_;
	}
	
	/*------------------------------------------------------------------------
     * Validity
     * */
    public boolean isValid()
    {
        return (0 <= getFD());
    }

}
