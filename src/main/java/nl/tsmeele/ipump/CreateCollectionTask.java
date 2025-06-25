package nl.tsmeele.ipump;

import nl.tsmeele.log.Log;
import nl.tsmeele.myrods.api.AccessType;
import nl.tsmeele.myrods.api.CollInp;
import nl.tsmeele.myrods.api.KeyValPair;
import nl.tsmeele.myrods.api.ObjType;
import nl.tsmeele.myrods.high.Collection;
import nl.tsmeele.myrods.high.IrodsObject;
import nl.tsmeele.myrods.high.IrodsUser;

public class CreateCollectionTask extends Task {
	private Collection coll;

	public CreateCollectionTask(IrodsUser clientUser, boolean runAsAgent, String precondition, Collection coll) {
		super(clientUser, runAsAgent, precondition);
		this.coll = coll;
	}

	@Override
	public Boolean call() throws Exception {
		Log.debug("TASK CREATE COLL" + "\nFROM=" + coll.collName + "\n  TO=" + context.destCollectionPath(coll));
		String destCollPath = context.destCollectionPath(coll);
		// check if collection already exists
		context.dest.rcObjStat(destCollPath, ObjType.COLLECTION);
		if (!context.dest.error) {
			// collection exists, we're done
			Log.debug("Destination collection already exists: " + destCollPath);
			// unblock any tasks that have this collection as precondition
			context.scheduler.unblock(coll.getPath());
			return null;
		}
		
		// do we have sufficient privs to create the collection?
		String destParentCollPath = IrodsObject.parent(destCollPath);
		if (runAsAgent && !context.dest.checkAccess(clientUser.name, context.ctx.destLocalZone, ObjType.COLLECTION, 
				destParentCollPath, AccessType.WRITE)) {
			// running on behalf of the client user and this user lacks sufficient access to destination collection
			// resubmit this pump task using rodsadmin access, and exit here
			Log.debug("Resubmitting task PumpDataObject to run as rodsadmin for data object " + coll.getPath());
			rescheduleTaskAsAdmin();
			return null;
		}
		
		// try to create the collection
		KeyValPair condInput = new KeyValPair();
		CollInp collInp = new CollInp(destCollPath, 0, 0, condInput);
		context.dest.rcCollCreate(collInp);
		if (context.dest.error) {
			Log.error("Unable to create destination collection '" + destCollPath + "', iRODS error = " + context.dest.intInfo);
			// creation failed, stop here
			return false;
		}
		Log.info("Destination collection created: " + destCollPath);

		// unblock any tasks that have this collection as precondition
		context.scheduler.unblock(coll.getPath());
		return null;
	}



}
