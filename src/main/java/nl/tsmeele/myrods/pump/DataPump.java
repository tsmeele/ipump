package nl.tsmeele.myrods.pump;

import java.io.IOException;
import java.util.ArrayList;

import nl.tsmeele.log.Log;
import nl.tsmeele.myrods.api.AccessType;
import nl.tsmeele.myrods.api.CollInp;
import nl.tsmeele.myrods.api.DataObjInp;
import nl.tsmeele.myrods.api.KeyValPair;
import nl.tsmeele.myrods.api.Kw;
import nl.tsmeele.myrods.api.ObjType;
import nl.tsmeele.myrods.api.RodsObjStat;
import nl.tsmeele.myrods.high.AVU;
import nl.tsmeele.myrods.high.DataObject;
import nl.tsmeele.myrods.high.DataTransfer;
import nl.tsmeele.myrods.high.DataTransferMultiThreaded;
import nl.tsmeele.myrods.high.DataTransferSingleThreaded;
import nl.tsmeele.myrods.high.PosixFileFactory;
import nl.tsmeele.myrods.high.Replica;
import nl.tsmeele.myrods.plumbing.MyRodsException;

/**
 * DataPump is responsible for copying a set of data objects that have the same creator (owner), along with AVU attributes,
 * residing underneath a collection in one iRODS zone to a collection in another another zone.
 * It attempts to map (preserve and transform) properties such as tree structure, object creator and object + collection AVU's.
 * @author ton
 *
 */
public class DataPump {
	/* The below static final value is a workaround for iRODS agent memory leaks:
	 * Unfortunately, iRODS agent processes are known to leak some memory. The amount depends on the type and number of 
	 * (policy) rules that are executed. Usually agents are short-lived and the memory leaks are not much of a concern.
	 * 
	 * However, in case we pump a large number of data objects, we would generate many iRODS API calls in a single, 
	 * long-running, session. To keep the impact of server-side memory leaks low, we will take the precaution to disconnect 
	 * and reconnect after a 'reasonable' amount of pump actions.  
	 */
	private static final int MAX_PUMP_ACTIONS_PER_SESSION = 1000;	// disconnect and reconnect when this threshold is reached
	private Context ctx;
	private String sLocalZone;
	private String dLocalZone;
	private DataObjectList list;
	private int threads = 1;
	
	
	public DataPump(Context ctx, String sLocalZone, String dLocalZone, DataObjectList list) {
		this.ctx = ctx;
		this.sLocalZone = sLocalZone;
		this.dLocalZone = dLocalZone;
		this.list = list;	
		String sThreads = ctx.options.get("threads");
		if (sThreads != null) {
			try {
				threads = Integer.parseInt(sThreads);
			} catch (NumberFormatException e) {
				// ignore illegal argument, use single thread
			}
			if (threads < 1) {
				threads = 1;
			}
		}
	}
	
	
	public void pump() throws MyRodsException, IOException {
		if (list.isEmpty()) return;
		Pirods source = new Pirods(ctx, true);
		Pirods destination = new Pirods(ctx, false);
		DataObject firstObj = list.get(0);
		String clientUsername = firstObj.ownerName;
		String clientZone = firstObj.ownerZone;
		
		// SOURCE LOGIN
		// if we are unable to login on source as clientUsername, we will use our proxyUser 
		// NB: Could be caused by creator of data object is no longer a user on the zone
		source.login(clientUsername, clientZone);
		boolean useProxySource = source.error;
		if (!useProxySource) {
			// check that the creator (clientUsername) has sufficient rights on the source objects, we test the first object (only)
			// if not, we will use the proxyuser instead
			useProxySource = !source.checkAccess(clientUsername, clientZone, ObjType.DATAOBJECT, firstObj.getPath(), AccessType.OWN);
		}
		boolean useProxyDestination = useProxySource;
		if (useProxySource) {
			source.rcDisconnect();	// need to reconnect as proxy user
			source.login();
			if (source.error) {
				Log.error(source.pUsername + " reconnect to source failed. iRODS error = " + source.intInfo);
				return;
			}
			Log.debug("Source logged in as proxy user");
		}
		// DESTINATION LOGIN
		// do not try to find an equivalent clientUser if it was a remote user on source
		// as this could cause inappropriate mapping
		if (!clientZone.equals(sLocalZone)) {
			Log.debug("Owner '" + firstObj.getOwner() + "' is not a local-zone user on source.");
			useProxyDestination = true;
		}
		// attempt to login on destination as equivalent client user
		if (!useProxyDestination) {
			destination.login(clientUsername, dLocalZone);
			if (destination.error) {
				Log.debug("Destination client user login failed, iRODS error = " + destination.intInfo);
				useProxyDestination = true;	// login failed, we will use proxy user instead
			}
		}
		// try proxy login if client user login is inappropriate or failed
		if (useProxyDestination) {
			destination.rcDisconnect();
			destination.login();
			if (destination.error) {
				Log.error(destination.pUsername + " reconnect to destination failed. iRODS error = " + destination.intInfo);
				return;
			}
			Log.debug("Destination logged in as proxy user");
		}
		
		// open a log to record the results of completed actions
		LogFile logFile = new LogFile(ctx.options.get("log"));
		
		// we will copy data objects within a sub-collection, establish path to the root of source and destination trees
		String sourceRoot;
		if (firstObj.getPath().equals(ctx.sourceObject)) {
			// a data object was specified as source
			sourceRoot = firstObj.collName;
		} else {
			// a collection was specified as source
			sourceRoot = ctx.sourceObject;
		}
		String destinationRoot = ctx.destinationCollection;
			
		// copy listed set of data objects, create destination collections on the fly where needed
		String currentCollection = "";
		int pumpActionsCount = 0;
		list.sortByPath();	// we traverse the tree depth-first, and create collections on the go
		
		for (DataObject obj : list) {
			pumpActionsCount++;
			Log.debug("pumpActionsCount = " + pumpActionsCount);
			// derive destination collection name from source path
			String sSubCollection = obj.collName.substring(sourceRoot.length());
			String dCollName;
			if (sSubCollection.length() > 0) {
				dCollName = destinationRoot + sSubCollection;
			} else {
				dCollName = destinationRoot;
			}
			// does this object reside in a collection that differs from the collection of the prior object?
			if (!obj.collName.equals(currentCollection)) {
				currentCollection = obj.collName;
				Log.debug("Start of destination collection " + dCollName);
				if (!dCollName.equals(destinationRoot)) {
					// 
					// create destination collection (if it does not yet exist) and copy collection AVU's
					if (!ensureCollectionExists(destination, dCollName)) {
						return;
					};
				}
			}
			// prepare replica references for this data object in source and destination
			String destObjPath = dCollName + "/" + obj.dataName;
			Log.info("...copying from " + obj.getPath() + " to " + destObjPath);
			Replica sourceReplica = PosixFileFactory.createReplica(source, obj.getPath());
			Replica destinationReplica = PosixFileFactory.createReplica(destination, destObjPath);
			if (destinationReplica.isFile()) {
				String message = "skipping, destination object already exists: '" + destObjPath + "'";
				logFile.LogError(obj.getPath(), message);
				Log.error(message);
				continue;
			}
			// (make sure we can) obtain all AVU's of source object
			// TODO: add to a future release
			//ArrayList<AVU> sourceAVUlist = source.getAvus("-d", obj.getPath());
			//Log.debug("object has " + sourceAVUlist.size() + " AVU's");
			
			// copy the data object
			DataTransfer tx = null;
			boolean transferError = false;
			if (threads < 2) {
				tx = new DataTransferSingleThreaded(sourceReplica, destinationReplica);
			} else {
				tx = new DataTransferMultiThreaded(sourceReplica, destinationReplica);
				((DataTransferMultiThreaded)tx).setThreads(threads);
			}
			try {
				tx.transfer();
			} catch (IOException e) {
				transferError = true;
				String message = "Transfer failed with exception: " + e.getMessage();
				logFile.LogError(obj.getPath(), message);
				Log.info(obj.getPath() + ": " + message);
			}
			
			// we will need to reconnect and login again
			// a) after a failed transfer
			// b) or as a precaution to counter iRODS agent memory leaks 
			if (transferError || pumpActionsCount > MAX_PUMP_ACTIONS_PER_SESSION) {
				pumpActionsCount = 0;
				source.rcDisconnect();
				destination.rcDisconnect();
				if (useProxySource) {
					source.login();
				} else {
					source.login(clientUsername, clientZone);
				}
				// only attempt destination login if source login was successful
				if (source.error) {
					Log.error("Reconnect to source server failed, iRODS error = " + source.intInfo);
				} else {
					if (useProxyDestination) {
						destination.login();
					} else {
						destination.login(clientUsername, dLocalZone);
					}
					if (destination.error) {
						Log.error("Reconnect to destination server failed, iRODS error = " + source.intInfo);
					}
				} 
				if (source.error || destination.error) {
					Log.info("Transfer of data objects for owner " + clientUsername + " aborted, due to reconnect failure");
					return;
				}
			}
			
			// analyze transfered data
			if (!transferError) {	
				// transfer was successful, check integrity of result using data size info
				RodsObjStat rodsObjStat = destination.rcObjStat(destObjPath, ObjType.DATAOBJECT);
				if (destination.error) {
					String message = "iRODS error: " + destination.intInfo + " (at destination) ";
					logFile.LogError(obj.getPath(), message);
					Log.error(message + " while checking info on '" + destObjPath + "'");
					transferError = true; // flag attempt to perform cleanup of partial data object
				}
				if (rodsObjStat.objSize == obj.dataSize) {
					Log.info("Copied and OK: " + obj.getPath());
					// copy/transform any AVUs that are linked to the data object
					// TODO: add to a future release
					//transferDataObjectAVUs(sourceAVUlist, destination, destObjPath);
					//Log.debug("AVUs copied");
					logFile.LogDone(obj.getPath());
				} else {
					String message = " copied replica size (" + rodsObjStat.objSize + ") does not match source size (" + obj.dataSize + ")";
					logFile.LogError(obj.getPath(), message);
					Log.error("Copy failed: " + obj.getPath() + message);
					transferError = true;	// flag cleanup needed of partial data object
				} 
			}
			// if transfer failed or resulting object is of insufficient quality,
			// attempt to remove the 'partial' data object at destination
			if (transferError) {
				DataObjInp dataObjInp = new DataObjInp(destObjPath, null);
				destination.rcDataObjUnlink(dataObjInp);
				if (destination.error) {
					Log.debug("unable to unlink '" + destObjPath + "' iRODS error = " + destination.intInfo);
				} else {
					Log.info("Cleaned up partial replica copy of data object at destination");
				}
			} 	
		} // end For
		source.rcDisconnect();
		destination.rcDisconnect();
	}
	
	private boolean transferDataObjectAVUs(ArrayList<AVU> sourceAVUlist, Pirods destination, String destObjPath) throws MyRodsException, IOException {
		if (sourceAVUlist.isEmpty()) return true;
		Log.debug("About to copy " + sourceAVUlist.size() + " AVUs from source to destination");
		// todo: might need to transform some AVU's here, e.g. to change a reference to the zone
		boolean result = destination.addAvus("-d", destObjPath, sourceAVUlist);
		if (!result) {
			Log.error("Unable to add AVUs to " + destObjPath + " iRODS error = " + destination.intInfo);
		}
		for (AVU avu: destination.getAvus("-d", destObjPath)) {
			Log.debug("AVU copied: " + avu.toString());
		}
		return result;
	}
	
	private boolean ensureCollectionExists(Pirods destination, String collName) throws MyRodsException, IOException {
		final int CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME = -809000;
		KeyValPair condInput = new KeyValPair();
		condInput.put(Kw.RECURSIVE_OPR__KW, "");	// create intermediate collections too 
		CollInp collInp = new CollInp(collName, 0, 0, condInput);
		destination.rcCollCreate(collInp);
		if (destination.error) {
			if (destination.intInfo == CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME) {
				// fine, collection already existed
				Log.debug("Destination collection already exists: " + collName);
				return true;
			}
			Log.error("Unable to create destination collection '" + collName + 
					"' iRODS error = " + destination.intInfo);
			return false;
		}
		Log.info("Destination collection created: " + collName);
		
		return true;
	}
	
	
	
	

}
