/*============================================================================
 20-Oct-2015    josep.sampe     	Initial implementation.
 17-Aug-2016	josep.sampe			Refactor
 ===========================================================================*/
package com.urv.vertigo.microcontroller;

import com.urv.vertigo.api.Api;
import com.urv.vertigo.context.Context;

public interface IMicrocontroller {
	public void invoke(Context ctx, Api api);
}
