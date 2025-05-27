package nl.tsmeele.myrods.pump;

import java.io.IOException;
import nl.tsmeele.myrods.api.Columns;
import nl.tsmeele.myrods.api.Flag;
import nl.tsmeele.myrods.api.GenQueryInp;
import nl.tsmeele.myrods.api.GenQueryOut;
import nl.tsmeele.myrods.api.InxIvalPair;
import nl.tsmeele.myrods.api.InxValPair;
import nl.tsmeele.myrods.api.KeyValPair;
import nl.tsmeele.myrods.high.DataObject;
import nl.tsmeele.myrods.high.Hirods;
import nl.tsmeele.myrods.irodsStructures.DataInt;
import nl.tsmeele.myrods.plumbing.MyRodsException;

public class Pirods extends Hirods {	
	public String pUsername, pZone;
	private String pPassword;
	private boolean authPam;

	
	public Pirods(Context ctx, boolean source) {
		super(	source ? ctx.sHost : ctx.dHost, 
				source ? ctx.sPort : ctx.dPort );
		if (source) {
			pUsername = ctx.sUsername;
			pZone = ctx.sZone;
			pPassword = ctx.sPassword;
			authPam = ctx.sAuthPam;
		} else {
			pUsername = ctx.dUsername;
			pZone = ctx.dZone;
			pPassword = ctx.dPassword;
			authPam = ctx.dAuthPam;
		}
	}
	
	public void login() throws MyRodsException, IOException {
		// proxy login as the proxy user itself
		login(pUsername, pZone);
	}
	
	public void login(String clientUsername, String clientZone) throws MyRodsException, IOException {
		// proxy login on behalf of a client user

		if (authPam) {
			pamLogin(pUsername, pZone, pPassword, clientUsername, clientZone);
		} else {
			nativeLogin(pUsername, pZone, pPassword, clientUsername, clientZone);
		}
	}
	
	public boolean isRodsAdmin() throws MyRodsException, IOException {
		if (!this.isAuthenticated()) return false;
		// SELECT clause
		InxIvalPair inxIvalPair = new InxIvalPair();
		inxIvalPair.put(Columns.USER_TYPE.getId(), Flag.SELECT_NORMAL);	
		// WHERE clause
		InxValPair inxValPair = new InxValPair();
		inxValPair.put(Columns.USER_NAME.getId(), "= '" + pUsername + "'");
		inxValPair.put(Columns.USER_ZONE.getId(), "= '" + pZone + "'");
		int maxRows = 1;
		GenQueryInp genQueryInp = new GenQueryInp(maxRows, 0, 0, Flag.AUTO_CLOSE,
				new KeyValPair(), inxIvalPair , inxValPair);
		GenQueryOut genOut = rcGenQuery(genQueryInp);
		if (error || genOut.columnCount < 1 || genOut.rowCount < 1) return false;
		String userType = genOut.data[0][0];
		return userType.equals("rodsadmin");
	}
	


	public DataObjectList selectDataObjects(String collectionName) throws MyRodsException, IOException {
		if (!isAuthenticated()) return null;
		// SELECT clause
		InxIvalPair inxIvalPair = new InxIvalPair();
		inxIvalPair.put(Columns.COLL_NAME.getId(), Flag.SELECT_NORMAL);	
		inxIvalPair.put(Columns.DATA_NAME.getId(), Flag.SELECT_NORMAL);	
		inxIvalPair.put(Columns.DATA_SIZE.getId(), Flag.SELECT_NORMAL);	
		inxIvalPair.put(Columns.DATA_OWNER_NAME.getId(), Flag.SELECT_NORMAL);	
		inxIvalPair.put(Columns.DATA_OWNER_ZONE.getId(), Flag.SELECT_NORMAL);	
		
		// WHERE clause to query data objects that are direct member of the collection
		InxValPair inxValPair = new InxValPair();
		inxValPair.put(Columns.COLL_NAME.getId(), "= '" + collectionName + "'");
		int maxRows = 256;
		GenQueryInp genQueryInp = new GenQueryInp(maxRows, 0, 0, 0,
				new KeyValPair(), inxIvalPair , inxValPair);
		DataObjectList out = executeQueryDataObjects(genQueryInp);
		
		// WHERE clause to query data objects in subcollections of the collection
		inxValPair = new InxValPair();
		inxValPair.put(Columns.COLL_NAME.getId(), "like '" + collectionName + "/%" + "'");
		genQueryInp = new GenQueryInp(maxRows, 0, 0, 0,
				new KeyValPair(), inxIvalPair , inxValPair);
		// return a union of the result of queries
		out.addAll(executeQueryDataObjects(genQueryInp));
		return out;
	}
	
	private DataObjectList executeQueryDataObjects(GenQueryInp genQueryInp) throws MyRodsException, IOException {
		DataObjectList list = new DataObjectList();
		boolean done = false;
		while (!done) {
			GenQueryOut genOut = rcGenQuery(genQueryInp);
			if (error) break;
			for (int i = 0; i < genOut.rowCount; i++) {
				DataObject obj = new DataObject(
						genOut.data[i][0],	// collName 
						genOut.data[i][1],	// dataName
						Long.parseLong(genOut.data[i][2]),	// dataSize
						genOut.data[i][3],	// ownerName
						genOut.data[i][4]);	// ownerZone
				list.add(obj);
			}

			// prepare for next set of rows
			DataInt continueInx = (DataInt) genQueryInp.lookupName("continueInx");
			continueInx.set(genOut.continueInx);
			if (genOut.rowCount < 256) {
				// last rowset received
				break;
			}
		}
		// close query
		DataInt maxRows = (DataInt) genQueryInp.lookupName("maxRows");
		maxRows.set(0);
		rcGenQuery(genQueryInp);
		return list;
	}
	
}
