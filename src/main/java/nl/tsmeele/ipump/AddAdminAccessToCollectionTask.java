package nl.tsmeele.ipump;

import nl.tsmeele.log.Log;
import nl.tsmeele.myrods.api.Kw;
import nl.tsmeele.myrods.api.ModAccessControlInp;
import nl.tsmeele.myrods.high.Collection;
import nl.tsmeele.myrods.high.IrodsUser;

public class AddAdminAccessToCollectionTask extends Task {
	private Collection coll;

	public AddAdminAccessToCollectionTask(IrodsUser clientUser, boolean runAsAgent, String precondition, Collection coll) {
		super(clientUser, runAsAgent, precondition);
		this.coll = coll;
	}

	@Override
	public Boolean call() throws Exception {
		Log.debug("TASK ADD ADMIN ACCESS TO COLL" + "\nFROM=" + coll.collName + "\n  TO=" + context.destCollectionPath(coll));
		
		// make sure this task runs as rodsadmin
		if (runAsAgent) {
			Log.debug("Resubmitting task AddAdminAccessToCollection as rodsadmin for collection " + coll.collName);
			rescheduleTaskAsAdmin();
			return null;
		}
		
		String destCollPath = context.destCollectionPath(coll);

		// ensure that admin has access to allow for subsequent operations
		ModAccessControlInp destAdminAccess   = new ModAccessControlInp(0, Kw.MOD_ADMIN_MODE_PREFIX + Kw.ACCESS_OWN, 
				context.ctx.dUserName, context.ctx.dZone, destCollPath);
		context.dest.rcModAccessControl(destAdminAccess);
		if (context.dest.error) {
			Log.error("Unable to add ACL for admin access on collection '" + destCollPath + "', iRODS error = " + context.dest.intInfo);
			// failed, stop here
			return false;
		}
		Log.debug("Own access for rodsadmin added to " + destCollPath);

		// unblock any tasks that require admin access to this collection set as precondition
		context.scheduler.unblock(Task.ADMIN_HAS_ACCESS + coll.getPath());
		return null;
	}



}
