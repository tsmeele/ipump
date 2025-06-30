package nl.tsmeele.ipump;


import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import nl.tsmeele.log.Log;
import nl.tsmeele.log.LogLevel;
import nl.tsmeele.myrods.api.Kw;
import nl.tsmeele.myrods.api.ModAccessControlInp;
import nl.tsmeele.myrods.api.ObjType;
import nl.tsmeele.myrods.api.RodsObjStat;
import nl.tsmeele.myrods.high.Collection;
import nl.tsmeele.myrods.high.DataObject;
import nl.tsmeele.myrods.high.Hirods;
import nl.tsmeele.myrods.high.IrodsObject;
import nl.tsmeele.myrods.high.IrodsUser;
import nl.tsmeele.myrods.plumbing.MyRodsException;

public class PumpMain {
	static nl.tsmeele.ipump.PumpContext ctx = null;
	static Hirods source, dest;
	
	public static void main(String[] args) throws IOException {
		// process command line arguments, will exit in case of error or usage request
		ctx = processCommandLineArgs(args);
		
		// login on both source and destination iRODS server
		Log.debug("before login & check privs");
		IrodsUser sourceAdmin = new IrodsUser(ctx.sUserName, ctx.sZone);
		IrodsUser destAdmin = new IrodsUser(ctx.dUserName, ctx.dZone);
		source = rodsAdminLogin(ctx.sHost, ctx.sPort, sourceAdmin, ctx.sPassword, ctx.sAuthPam);
		if (source == null) {
			System.exit(2);
		}
		dest   = rodsAdminLogin(ctx.dHost, ctx.dPort, destAdmin, ctx.dPassword, ctx.dAuthPam);
		if (dest == null) {
			source.rcDisconnect();
			System.exit(3);
		}
		Log.info("Logged in with rodsadmin privs on source and destination server");
		
		// If requested, find objects in resume log that have been processed already, we can skip those
		final Set<String> excludeList;
		if (ctx.resume) {
			excludeList = LogFile.slurpCompletedObjects(ctx.resumeFile);
			if (excludeList == null) {
				Log.error("Unable to open resume file '" + ctx.resumeFile + "'");
				System.exit(4);
			}
			Log.debug("Resume file analyzed");
		} else {
			excludeList = new HashSet<String>();
		}
		
		// assert command line argument source/destination objects (+ type) exist on relevant server
		ctx.sourceObject = statObject(source, ctx.sourceObjPath);
		ctx.destObject = statObject(dest, ctx.destinationCollPath);			
		if (ctx.sourceObject == null || ctx.destObject == null) {
			closeConnectionsAndExit(5);
		}
		if (ctx.destObject.getClass() != Collection.class) {
			Log.error("Destination object may not be of type data object (must be a collection).");
			closeConnectionsAndExit(6);
		}
		
		boolean yodaError = false;
		// Yoda specific: refuse processing if an active workflow is found in source collection tree
		if (ctx.sourceObject.isCollection()) {
			List<Collection> workflows = nl.tsmeele.ipump.IrodsQuery.findYodaWorkflowInCollections(source, ctx.sourceObjPath);
			if (!workflows.isEmpty()) {
				yodaError = true;
				Log.error("Unable to proceed, because source collection has active Yoda workflow:");
				for (Collection coll : workflows) {
					Log.info("Active workflow collection = " + coll.collName);
				}
			}
		}
		
		// Yoda specific: refuse processing if an active lock is found in source collection tree
		if (ctx.sourceObject.isCollection()) {
			List<Collection> locks = IrodsQuery.findYodaLockedCollections(source, ctx.sourceObjPath);
			if (!locks.isEmpty()) {
				yodaError = true;
				Log.error("Unable to proceed, because source collection has Yoda lock:");
				for (Collection coll : locks) {
					Log.info("Active lock collection = " + coll.collName);
				}
			}
		}
		
		if (yodaError) {
			closeConnectionsAndExit(7);
		}
		
		// create a list of all objects on the source server that will need to be transferred
		List<DataObject> dataList = new ArrayList<DataObject>();
		List<Collection> collList = new ArrayList<Collection>();
		if (ctx.sourceObject.isDataObject()) {
			Log.debug("Source object is a data object");
			// user wants just a single data object to be transferred
			dataList.add((DataObject)ctx.sourceObject);
		} else {
			// collect all objects located in the source collection and its subcollections
			Log.debug("Source object is a collection (will query to obtain a list of its members)");
			dataList = IrodsQuery.getDataObjects(source, ctx.sourceObject.getPath(), true);
			collList = IrodsQuery.getSubCollections(source, ctx.sourceObject.getPath(), true);
		}

		// remove objects listed in our exclude list (contains successful transfers as per resume log)
		int dataCount = dataList.size();
		int collCount = collList.size();
		Log.info("Source object consists of " + dataList.size() + " data objects and " + collList.size() + " subcollections");
		dataList.removeIf(obj -> excludeList.contains(obj.getPath()));
		collList.removeIf(obj -> excludeList.contains(obj.getPath()));
		int dataDiff = dataCount - dataList.size();
		int collDiff = collCount - collList.size();
		if (dataDiff > 0) {
			Log.info("Skipping " + dataDiff + " data objects found in resume log");
		}
		if (collDiff > 0) {
			Log.info("Skipping " + collDiff + " subsollections found in resume log");
		}
		
		// assemble a set of unique object creators, we aim to act on their behalf during the transfers 
		HashSet<IrodsUser> creators = new HashSet<IrodsUser>();
		for (IrodsObject obj : dataList) {
			creators.add(obj.owner);
		}
		for (IrodsObject obj : collList) {
			creators.add(obj.owner);
		}
		
		/* 
		 * remove creators from this set if: 
		 * a) they no longer exist on source or destination 
		 * b) if it is a non-local account on the source, since we cannot map them
		 *   (in that case we probably should not assume user#sourceZone == user#destZone) 
		 */
		ctx.sourceLocalZone = source.getLocalZone();
		ctx.destLocalZone = dest.getLocalZone();
		if (ctx.sourceLocalZone == null || ctx.destLocalZone == null) {
			Log.error("Unable to obtain localzone name from source and/or destination server");
			closeConnectionsAndExit(8);	
		}
		// (b) remove non-local users
		int nCreators = creators.size();
		creators.removeIf(user -> !user.zone.equals(ctx.sourceLocalZone));
		if (creators.size() < nCreators) {
			Log.debug("Found " + (nCreators - creators.size()) + " non-local object(s) owners on source server");
			nCreators = creators.size();
		}
		// (a) remove users that do not exist on source or destination
		for (Iterator<IrodsUser> it = creators.iterator(); it.hasNext();) {
			IrodsUser user = it.next();
			if (source.getUserType(user.name, ctx.sourceLocalZone) == null
					|| dest.getUserType(user.name, ctx.destLocalZone) == null) {
				it.remove();
			}
		}
		if (creators.size() < nCreators) {
			Log.debug("Found " + (nCreators - creators.size())
					+ " object(s) owners that do not exist on source or destination");
		}
		
		// schedule tasks for each object
		int clientThreads = 2;
		TaskScheduler scheduler = new TaskScheduler(ctx, clientThreads);
		for (Collection coll : collList) {
			IrodsUser agent = sourceAdmin;
			boolean runAsAgent = false;
			if (creators.contains(coll.owner)) {
				agent = coll.owner;
				runAsAgent = true;
			}
			// a collection can be created once its parent collection exists
			scheduler.addBlockedTask(new CreateCollectionTask(agent, runAsAgent, coll.getParentPath(), coll));
			// admin access can be added to a collection once the collection exists
			scheduler.addBlockedTask(new AddAdminAccessToCollectionTask(sourceAdmin, false, coll.getPath(), coll));
			// AVUs can be added to a collection once the rodsadmin has sufficient access to that collection
			scheduler.addBlockedTask(new AddCollectionAvusTask(sourceAdmin, false, Task.ADMIN_HAS_ACCESS + coll.getPath(), coll));
			// if needed, add reminder in logfile to republish a data package once AVUs have been added to the collection
			scheduler.addBlockedTask(new RepublishCollectionTask(sourceAdmin, false, Task.AVU_ADDED + coll.getPath(), coll));
			// Log collection done once any republication reminder has been processed for the collection
			scheduler.addBlockedTask(new LogCollectionDoneTask(sourceAdmin, false, Task.REPUBLISHED + coll.getPath(), coll));
		}
		for (DataObject data : dataList) {
			IrodsUser agent = sourceAdmin;
			boolean runAsAgent = false;
			if (creators.contains(data.owner)) {
				agent = data.owner;
				runAsAgent = true;
			}
			// a data object can be copied once the rodsadmin has sufficient access to the collection in which it will reside
			scheduler.addBlockedTask(new PumpDataObjectTask(agent, runAsAgent, Task.ADMIN_HAS_ACCESS + data.getParentPath(), data));
			// admin access can be added to a data object once the object exists AND the admin has access to the collection
			// in which the object resides (the second precondition is implicitly fulfilled)
			scheduler.addBlockedTask(new AddAdminAccessToDataObjectTask(sourceAdmin, false, data.getPath(), data));
			// AVUs can be added to a data object once the rodsadmin has sufficient access to that object
			scheduler.addBlockedTask(new AddDataObjectAvusTask(sourceAdmin, false, Task.ADMIN_HAS_ACCESS + data.getPath(), data));
			// Log data object done once AVUs have been added to the object
			scheduler.addBlockedTask(new LogDataObjectDoneTask(sourceAdmin, false, Task.AVU_ADDED + data.getPath(), data));

		}
		if (ctx.sourceObject.isCollection()) {
			// unblock transfers for data objects that reside directly underneath the source collection 
			scheduler.unblock(Task.ADMIN_HAS_ACCESS + ctx.sourceObject.getPath());
			// unblock transfers for subcollections that reside directly underneath the source collection
			scheduler.unblock(ctx.sourceObject.getPath());
		} else {
			// unblock transfer of the data object
			scheduler.unblock(Task.ADMIN_HAS_ACCESS + ctx.sourceObject.getParentPath());
		}
		
		/* Add admin access to source and destination recursively, this fulfills the initial preconditions
		 * and ensures that objects can be processed despite a client user lacking sufficient access.
		 * Later, ipump will create new objects in destination, for which admin access will be set separately during the process.
		 * TODO: Should remove these ACL's after all ipump operations are done
		 */
		ModAccessControlInp sourceAdminAccess;
		final int recursive = 1;
		if (ctx.sourceObject.isCollection()) {
			sourceAdminAccess = new ModAccessControlInp(recursive, Kw.MOD_ADMIN_MODE_PREFIX + Kw.ACCESS_OWN, 
				ctx.sUserName, ctx.sZone,  ctx.sourceObjPath);
		} else {
			sourceAdminAccess = new ModAccessControlInp(recursive, Kw.MOD_ADMIN_MODE_PREFIX + Kw.ACCESS_OWN, 
					ctx.sUserName, ctx.sZone,  ctx.sourceObject.getParentPath());
		}
		ModAccessControlInp destAdminAccess   = new ModAccessControlInp(recursive, Kw.MOD_ADMIN_MODE_PREFIX +Kw.ACCESS_OWN, 
				ctx.dUserName, ctx.dZone,  ctx.destinationCollPath);
		source.rcModAccessControl(sourceAdminAccess);
		if (source.error) {
			Log.error("Unable to add own access on source object for user " + sourceAdmin.nameAndZone() + " iRODS error = " + source.intInfo);
		} else {
			dest.rcModAccessControl(destAdminAccess);
			if (dest.error) {
				Log.error("Unable to add own access on destination collection for user " + destAdmin.nameAndZone() + " iRODS error = " + dest.intInfo);
			}
		}
		if (source.error || dest.error) {
			closeConnectionsAndExit(9);
		}
		
		// close current connections, we're done with preparations
		source.rcDisconnect();
		dest.rcDisconnect();
		
		// open a log to record transfer results
		ctx.log = new LogFile(ctx.logFile);

		// execute transfers
		Log.info("About to transfer " + dataList.size() + " data objects and create " + collList.size() + " subcollections");
		long startCopy = timeStamp();
		scheduler.runTasks();
		long elapsed = timeStamp() - startCopy;
		Log.info("Total elapsed time " + elapsed + " seconds");
		
		int blocked = scheduler.countBlockedObjects();
		if (blocked > 0) {
			Log.warning("Due to transfer errors at parent, there are remaing tasks for " + blocked + " objects.");
			Log.debug(scheduler.toString());
		}
		
		// close the log with transfers results
		ctx.log.close();
	}
	
		
	
	private static PumpContext processCommandLineArgs(String[] args) {
		PumpContext ctx = new PumpContext();
		// analyze command line arguments
		try {
			ctx.processArgs(args);
		} catch (MyRodsException e) {
			System.err.println(e.getMessage() + "\n");
			ctx.usage = true;
		}
		// process command line options
		if (ctx.usage) {
			System.out.println(ctx.usage());
			System.exit(1);
		}
		if (ctx.verbose) Log.setLoglevel(LogLevel.INFO);
		if (ctx.debug) {
			Log.setLoglevel(LogLevel.DEBUG);
			String[] classFilter = { "nl.tsmeele.ipump" };
			Log.setDebugOutputFilter(classFilter);
		}
		// ensure that we got sufficient command line arguments
		if (ctx.sourceObjPath == null || ctx.destinationCollPath == null) {
			Log.error("Missing commandline argument for source and/or destination");
			System.exit(1);
		}
		return ctx;
	}
	
	private static Hirods rodsAdminLogin(String host, int port, IrodsUser user, String password, boolean authPam)  {
		Hirods hirods = new Hirods(host, port);
		boolean success = false;
		try {
			if (authPam) {
				success = hirods.pamLogin(user.name, user.zone, password, user.name, user.zone);
			} else {
				success = hirods.nativeLogin(user.name, user.zone, password, user.name, user.zone);
			}
			if (success) {
				// also make sure that user is a rodsadmin
				String userType = hirods.getUserType(user.name, user.zone);
				if (userType != null && userType.equals("rodsadmin")) {
					return hirods;
				} else {
					Log.error("User '" + user.nameAndZone() + "' lacks rodsadmin privileges on host " + host);
					success = false;
				}
			} else { 
				Log.error("Unable to connect and/or login to " + host + " as " + user.nameAndZone() + " iRODS error: " + hirods.intInfo);
			}
			// rodsAdminLogin failed, attempt to clean up the connection
			hirods.rcDisconnect();
		} catch (IOException e) { 
			Log.error(e.getMessage());
		}
		return null;
	}
	
	private static IrodsObject statObject(Hirods hirods, String path) throws MyRodsException, IOException {
		RodsObjStat objStat;
		objStat = hirods.rcObjStat(path, null);
		if (hirods.error) {
			Log.error("Unable to find object " + path + "', iRODS error = " + hirods.intInfo);
			return null;
		}
		if (ObjType.lookup(objStat.objType) == ObjType.COLLECTION) {
			return new Collection(path, objStat.ownerName, objStat.ownerZone);
		} 
		return new DataObject(IrodsObject.parent(path), IrodsObject.basename(path), objStat.objSize, objStat.ownerName, objStat.ownerZone);
	}
	
	private static void closeConnectionsAndExit(int code) {
		try {
			source.rcDisconnect();
		} catch (Exception e) {}
		try {
			dest.rcDisconnect();
		} catch (Exception e) {}
		System.exit(code);
	}
	
	private static long timeStamp() {
		return Instant.now().getEpochSecond();
	}

}
