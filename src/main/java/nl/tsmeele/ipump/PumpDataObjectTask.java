package nl.tsmeele.ipump;

import java.io.IOException;

import nl.tsmeele.log.Log;
import nl.tsmeele.myrods.api.AccessType;
import nl.tsmeele.myrods.api.DataObjInp;
import nl.tsmeele.myrods.api.ObjType;
import nl.tsmeele.myrods.api.RodsObjStat;
import nl.tsmeele.myrods.high.DataObject;
import nl.tsmeele.myrods.high.DataTransfer;
import nl.tsmeele.myrods.high.DataTransferMultiThreaded;
import nl.tsmeele.myrods.high.DataTransferSingleThreaded;
import nl.tsmeele.myrods.high.IrodsUser;
import nl.tsmeele.myrods.high.PosixFileFactory;
import nl.tsmeele.myrods.high.Replica;

public class PumpDataObjectTask extends Task {
	private DataObject dataObj;

	public PumpDataObjectTask(IrodsUser clientUser, boolean runAsAgent, String precondition, DataObject dataObj) {
		super(clientUser, runAsAgent, precondition);
		this.dataObj = dataObj;
	}

	@Override
	public Boolean call() throws Exception {
		String destPath = context.destCollectionPath(dataObj) + "/" + dataObj.dataName;
		Log.debug("TASK TRANSFER DATA OBJECT\nFROM=" + dataObj.getPath() + "\n  TO=" + destPath );
		
		// do we have sufficient privs to perform the transfer? 
		if (runAsAgent && !context.dest.checkAccess(clientUser.name, context.ctx.destLocalZone, ObjType.COLLECTION, 
				context.destCollectionPath(dataObj), AccessType.WRITE)) {
			Log.debug("INSUFF ACCESS: " + clientUser.name + "#" + context.ctx.destLocalZone + " path=" + context.destCollectionPath(dataObj) );
			// running on behalf of the client user and this user lacks sufficient access to destination collection
			// resubmit this pump task using rodsadmin access, and exit here
			Log.debug("Resubmitting task PumpDataObject to run as rodsadmin for data object " + dataObj.getPath());
			rescheduleTaskAsAdmin();
			return null;
		}
		
		
		Log.info("...copying from " + dataObj.getPath() + " to " + destPath);
		
		Replica sourceReplica = PosixFileFactory.createReplica(context.source, dataObj.getPath());
		Replica destReplica = PosixFileFactory.createReplica(context.dest, destPath);
		
		// copy the data object, but do not overwrite any existing object at destination
		boolean alreadyExists = false;
		if (destReplica.isFile()) {
			alreadyExists = true;
			String msg = "Skipping transfer as destination object already exists: '" + destPath + "'";
			Log.info(msg);
		} else {
			// copy the data object
			DataTransfer tx = null;
			if (context.ctx.threads < 2) {
				tx = new DataTransferSingleThreaded(sourceReplica, destReplica);
			} else {
				tx = new DataTransferMultiThreaded(sourceReplica, destReplica);
				((DataTransferMultiThreaded) tx).setThreads(context.ctx.threads);
			}
			try {
				tx.transfer();
			} catch (IOException e) {
				String msg = "Transfer failed with exception: " + e.getMessage();
				context.ctx.log.logError(dataObj.getPath(), msg);
				Log.info(dataObj.getPath() + ": " + msg);
				removePartialObject(destPath);
				// the failed transfer makes current connection unreliable, signal next task needs a new connection
				context.disconnect();
				return false;
			}
		}
		
		// assert that all content of data object now is present at destination
		// running a checksum is time-intensive on large objects, we will use data size instead
		RodsObjStat objStat = context.dest.rcObjStat(destPath, ObjType.DATAOBJECT);
		if (context.dest.error) {
			String msg = "iRODS error: " + context.dest.intInfo + " (at destination)";
			context.ctx.log.logError(dataObj.getPath(), msg);
			Log.error(msg + " while asserting destination object size for '" + destPath + "'" );
			return false;
		}
		if (objStat.objSize != dataObj.dataSize) {
			String msg = "destination size is " + objStat.objSize + " while source size is " + dataObj.dataSize + " (" + dataObj.getPath() + ")";
			if (alreadyExists) {
				Log.error("Existing object copy size mismatch: " + msg);
			} else {
				Log.error("Transferred object copy size mismatch: " + msg);
				removePartialObject(destPath);
			}
			context.ctx.log.logError(dataObj.getPath(), msg);
			return false;
		}
		
		// transfer is successful
		if (alreadyExists) {
			Log.info("Copy exists and matches source object: " + dataObj.getPath());
		} else {
			Log.info("Copied and OK: " + dataObj.getPath());
		}
		context.ctx.log.logDone(dataObj.getPath());
		
		// unblock queued tasks that have this object as precondition
		context.scheduler.unblock(dataObj.getPath());
		return null;
	}

	private void removePartialObject(String destPath) {
		DataObjInp dataObjInp = new DataObjInp(destPath, null);
		try {
			context.dest.rcDataObjUnlink(dataObjInp);
			if (context.dest.error) {
				Log.debug("Unable to unlink '" + destPath + "' iRODS error = " + context.dest.intInfo);
			} else {
				Log.info("Cleaned up partial replica copy of data object at destination");
			}
		} catch (IOException e) {
			// current connection has become unreliable, signal next task needs a new connection
			context.disconnect();
		}
	}
	
	public String toString() {
		return super.toString() + " obj = " + dataObj.getPath();
	}

}
