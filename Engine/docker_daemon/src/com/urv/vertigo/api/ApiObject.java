/*============================================================================
 21-Oct-2015    josep.sampe			Initial implementation.
 05-feb-2016	josep.sampe			Added Internal Client.
 17-Aug-2016	josep.sampe			Refactor 
 ===========================================================================*/
package com.urv.vertigo.daemon;

import java.util.Map;

public class ApiObject {
	public Map<String, String> metadata;

	public ApiObject(Map<String, String> objectMetadata) {
		metadata = objectMetadata;
	}
}