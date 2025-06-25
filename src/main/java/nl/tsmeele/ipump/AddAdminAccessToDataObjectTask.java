package nl.tsmeele.ipump;

import nl.tsmeele.log.Log;
import nl.tsmeele.myrods.api.Kw;
import nl.tsmeele.myrods.api.ModAccessControlInp;
import nl.tsmeele.myrods.high.DataObject;
import nl.tsmeele.myrods.high.IrodsUser;

public class AddAdminAccessToDataObjectTask extends Task {
	private DataObject dataObj;

	public AddAdminAccessToDataObjectTask(IrodsUser clientUser, boolean runAsAgent, String precondition, DataObject dataObj) {
		super(clientUser, runAsAgent, precondition);
		this.dataObj = dataObj;
	}

	@Override
	public Boolean call() throws Exception {
		String destPath = context.destCollectionPath(dataObj) + "/" + dataObj.dataName;
		Log.debug("TASK ADD ADMIN ACCESS TO DATAOBJECT" + "\nFROM=" + dataObj.getPath() + "\n  TO=" + destPath);
		
		// make sure this task runs as rodsadmin
		if (runAsAgent) {
			Log.debug("Resubmitting task AddAdminAccessToDataObject as rodsadmin for data object " + dataObj.getPath());
			rescheduleTaskAsAdmin();
			return null;
		}

		// ensure that admin has access to allow for subsequent operations
		ModAccessControlInp destAdminAccess   = new ModAccessControlInp(0, Kw.MOD_ADMIN_MODE_PREFIX + Kw.ACCESS_OWN, 
				context.ctx.dUserName, context.ctx.dZone, destPath);
		context.dest.rcModAccessControl(destAdminAccess);
		if (context.dest.error) {
			Log.error("Unable to add ACL for admin access on data object '" + destPath + "', iRODS error = " + context.dest.intInfo);
			// failed, stop here
			return false;
		}
		Log.debug("Own access for rodsadmin added to " + destPath);

		// unblock any tasks that require admin access to this data object set as precondition
		context.scheduler.unblock(Task.ADMIN_HAS_ACCESS + dataObj.getPath());
		return null;
	}



}
