package nl.tsmeele.ipump;

import nl.tsmeele.log.Log;
import nl.tsmeele.myrods.high.DataObject;
import nl.tsmeele.myrods.high.IrodsUser;

public class LogDataObjectDoneTask extends Task {
	private DataObject dataObj;

	public LogDataObjectDoneTask(IrodsUser clientUser, boolean runAsAgent, String precondition, DataObject dataObj) {
		super(clientUser, runAsAgent, precondition);
		this.dataObj = dataObj;
	}

	@Override
	public Boolean call() throws Exception {
		Log.debug("TASK LOG DATA OBJECT DONE " +  dataObj.getPath());	
		context.ctx.log.logDone(dataObj.getPath());
		return null;
	}


	
}
