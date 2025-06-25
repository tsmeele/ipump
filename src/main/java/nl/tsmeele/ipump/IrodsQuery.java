package nl.tsmeele.ipump;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import nl.tsmeele.myrods.api.Columns;
import nl.tsmeele.myrods.api.Flag;
import nl.tsmeele.myrods.api.GenQueryInp;
import nl.tsmeele.myrods.api.GenQueryOut;
import nl.tsmeele.myrods.api.InxIvalPair;
import nl.tsmeele.myrods.api.InxValPair;
import nl.tsmeele.myrods.api.KeyValPair;
import nl.tsmeele.myrods.high.Collection;
import nl.tsmeele.myrods.high.DataObject;
import nl.tsmeele.myrods.high.Hirods;
import nl.tsmeele.myrods.plumbing.MyRodsException;

public class IrodsQuery {
	public final static String ORG_LOCK = "org_lock";
	public final static String ORG_STATUS = "org_status";
	public final static String SECURED = "SECURED";
	public final static String EMPTY = "";
	
	
	public static List<Collection> findYodaLockedCollections(Hirods hirods, String collPath) throws MyRodsException, IOException {
		if (!hirods.isAuthenticated()) return null;
		List<Collection> out = new ArrayList<Collection>();
		// select clause
		InxIvalPair inxIvalPair = new InxIvalPair();
		inxIvalPair.put(Columns.COLL_NAME.getId(), Flag.SELECT_NORMAL);	
		inxIvalPair.put(Columns.COLL_OWNER_NAME.getId(), Flag.SELECT_NORMAL);	
		inxIvalPair.put(Columns.COLL_OWNER_ZONE.getId(), Flag.SELECT_NORMAL);	
		// where clause for collection
		InxValPair inxValPairColl = new InxValPair();
		inxValPairColl.put(Columns.COLL_NAME.getId(), "= '" + collPath + "'");
		inxValPairColl.put(Columns.META_COLL_ATTR_NAME.getId(), "= '" + ORG_LOCK + "'");
		// where clause for all subcollections
		InxValPair inxValPairSub  = new InxValPair();
		inxValPairSub.put(Columns.COLL_NAME.getId(), "like '" + collPath + "/%" + "'");
		inxValPairSub.put(Columns.META_COLL_ATTR_NAME.getId(), "= '" + ORG_LOCK + "'");

		int maxRows = 256;

		// query the collection itself
		GenQueryInp genQueryInp = new GenQueryInp(maxRows, 0, 0, 0,
				new KeyValPair(), inxIvalPair , inxValPairColl);
		out.addAll(runGetCollectionQuery(hirods, genQueryInp));
		// query subcollections
		genQueryInp = new GenQueryInp(maxRows, 0, 0, 0,
				new KeyValPair(), inxIvalPair , inxValPairSub);
		out.addAll(runGetCollectionQuery(hirods, genQueryInp));
		return out;
	}
	
	
	public static List<Collection> findYodaWorkflowInCollections(Hirods hirods, String collPath) throws MyRodsException, IOException {
		if (!hirods.isAuthenticated()) return null;
		List<Collection> out = new ArrayList<Collection>();
		// select clause
		InxIvalPair inxIvalPair = new InxIvalPair();
		inxIvalPair.put(Columns.COLL_NAME.getId(), Flag.SELECT_NORMAL);	
		inxIvalPair.put(Columns.COLL_OWNER_NAME.getId(), Flag.SELECT_NORMAL);	
		inxIvalPair.put(Columns.COLL_OWNER_ZONE.getId(), Flag.SELECT_NORMAL);	
		// where clause for collection
		InxValPair inxValPairColl = new InxValPair();
		inxValPairColl.put(Columns.COLL_NAME.getId(), "= '" + collPath + "'");
		inxValPairColl.put(Columns.META_COLL_ATTR_NAME.getId(), "= '" + ORG_STATUS + "'");
		inxValPairColl.put(Columns.META_COLL_ATTR_VALUE.getId(), "!= '" + SECURED + "'");
		inxValPairColl.put(Columns.META_COLL_ATTR_VALUE.getId(), "!= '" + EMPTY + "'");
		// where clause for all subcollections
		InxValPair inxValPairSub  = new InxValPair();
		inxValPairSub.put(Columns.COLL_NAME.getId(), "like '" + collPath + "/%" + "'");
		inxValPairSub.put(Columns.META_COLL_ATTR_NAME.getId(), "= '" + ORG_STATUS + "'");
		inxValPairSub.put(Columns.META_COLL_ATTR_VALUE.getId(), "!= '" + SECURED + "'");
		inxValPairSub.put(Columns.META_COLL_ATTR_VALUE.getId(), "!= '" + EMPTY + "'");
		int maxRows = 256;

		// query the collection itself
		GenQueryInp genQueryInp = new GenQueryInp(maxRows, 0, 0, 0,
				new KeyValPair(), inxIvalPair , inxValPairColl);
		out.addAll(runGetCollectionQuery(hirods, genQueryInp));
		// query subcollections
		genQueryInp = new GenQueryInp(maxRows, 0, 0, 0,
				new KeyValPair(), inxIvalPair , inxValPairSub);
		out.addAll(runGetCollectionQuery(hirods, genQueryInp));
		return out;
	}
	
	
	public static List<Collection> getSubCollections(Hirods hirods, String collPath, boolean recursive) throws MyRodsException, IOException {
		if (!hirods.isAuthenticated()) return null;
		List<Collection> out = new ArrayList<Collection>();
		// SELECT clause
		InxIvalPair inxIvalPair = new InxIvalPair();
		inxIvalPair.put(Columns.COLL_NAME.getId(), Flag.SELECT_NORMAL);	
		inxIvalPair.put(Columns.COLL_OWNER_NAME.getId(), Flag.SELECT_NORMAL);	
		inxIvalPair.put(Columns.COLL_OWNER_ZONE.getId(), Flag.SELECT_NORMAL);	
		/* WHERE clause to query data objects that are either 
		 * a) only direct members of the collection (recursive == false)
		 * b) all direct and indirect members of the collection (recursive == true)
		 */	
		InxValPair inxValPair = new InxValPair();
		if (recursive) {
			inxValPair.put(Columns.COLL_NAME.getId(), "like '" + collPath + "/%" + "'");
		} else {
			inxValPair.put(Columns.COLL_PARENT_NAME.getId(), "= '" + collPath + "'");
		}
		int maxRows = 256;
		GenQueryInp genQueryInp = new GenQueryInp(maxRows, 0, 0, 0,
				new KeyValPair(), inxIvalPair , inxValPair);
		out.addAll(runGetCollectionQuery(hirods, genQueryInp));

		return out;
	}
	
	public static List<DataObject> getDataObjects(Hirods hirods, String collPath, boolean recursive) throws MyRodsException, IOException {
		if (!hirods.isAuthenticated()) return null;
		List<DataObject> out = new ArrayList<DataObject>();
		// SELECT clause
		InxIvalPair inxIvalPair = new InxIvalPair();
		inxIvalPair.put(Columns.COLL_NAME.getId(), Flag.SELECT_NORMAL);
		inxIvalPair.put(Columns.DATA_NAME.getId(), Flag.SELECT_NORMAL);	
		inxIvalPair.put(Columns.DATA_SIZE.getId(), Flag.SELECT_NORMAL);	
		inxIvalPair.put(Columns.DATA_OWNER_NAME.getId(), Flag.SELECT_NORMAL);	
		inxIvalPair.put(Columns.DATA_OWNER_ZONE.getId(), Flag.SELECT_NORMAL);
		/* first WHERE clause to query data objects that are
		 * only direct members of the collection (always requested)
		 */
		InxValPair inxValPair = new InxValPair();
		inxValPair.put(Columns.COLL_NAME.getId(), "= '" + collPath + "'");
		
		int maxRows = 256;
		GenQueryInp genQueryInp = new GenQueryInp(maxRows, 0, 0, 0,
				new KeyValPair(), inxIvalPair , inxValPair);
		out.addAll(runGetDataObjectQuery(hirods, genQueryInp));

		if (!recursive) {
			return out;
		}
		/* second WHERE clause to query data objects that are
		 * indirect members of the collection (recursive == true)	
		 */
		inxValPair = new InxValPair();
		inxValPair.put(Columns.COLL_NAME.getId(), "like '" + collPath + "/%" + "'");
		
		genQueryInp = new GenQueryInp(maxRows, 0, 0, 0,
				new KeyValPair(), inxIvalPair , inxValPair);
		out.addAll(runGetDataObjectQuery(hirods, genQueryInp));
		return out;
	}
	
	private static List<Collection> runGetCollectionQuery(Hirods hirods, GenQueryInp genQueryInp) throws MyRodsException, IOException {
		List<Collection> out = new ArrayList<Collection>();
		Iterator<GenQueryOut> it = hirods.genQueryIterator(genQueryInp);
		while (it.hasNext()) {
			GenQueryOut genOut = it.next();
			for (int i = 0; i < genOut.rowCount; i++) {
				Collection coll = new Collection(
					genOut.data[i][0], // collName
					genOut.data[i][1], // collOwnerName
					genOut.data[i][2]); // collOwnerZone
				out.add(coll);
			}
		}
		return out;
	}
	
	private static List<DataObject> runGetDataObjectQuery(Hirods hirods, GenQueryInp genQueryInp) throws MyRodsException, IOException {
		List<DataObject> out = new ArrayList<DataObject>();
		Iterator<GenQueryOut> it = hirods.genQueryIterator(genQueryInp);
		while (it.hasNext()) {
			GenQueryOut genOut = it.next();
			for (int i = 0; i < genOut.rowCount; i++) {
				DataObject obj = new DataObject(
						genOut.data[i][0],	// collName 
						genOut.data[i][1],	// dataName
						Long.parseLong(genOut.data[i][2]),	// dataSize
						genOut.data[i][3],	// dataOwnerName
						genOut.data[i][4]);	// dataOwnerZone
				out.add(obj);
			}
		}
		return out;
	}
	
	
	
	
	
	
	
}
