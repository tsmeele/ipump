package nl.tsmeele.ipump;

import nl.tsmeele.log.Log;
import nl.tsmeele.myrods.high.Collection;
import nl.tsmeele.myrods.high.IrodsUser;

public class LogCollectionDoneTask extends Task {
	private Collection coll;

	public LogCollectionDoneTask(IrodsUser clientUser, boolean runAsAgent, String precondition, Collection coll) {
		super(clientUser, runAsAgent, precondition);
		this.coll = coll;
	}

	@Override
	public Boolean call() throws Exception {
		Log.debug("TASK LOG COLLECTION DONE " +  coll.getPath());	
		context.ctx.log.logDone(coll.getPath());
		return null;
	}


	
}
