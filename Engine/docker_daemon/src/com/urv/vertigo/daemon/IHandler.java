/*============================================================================
 20-Oct-2015    josep.sampe       Initial implementation.
 ===========================================================================*/
package com.urv.vertigo.daemon;

import java.util.Map;

public interface IHandler {
	public void invoke(HandlerLogger logger, HandlerMetadata metadata, Map<String, String> file_md, Map<String, String> req_md,  HandlerOutput out);
}
