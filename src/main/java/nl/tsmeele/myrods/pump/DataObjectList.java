package nl.tsmeele.myrods.pump;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

import nl.tsmeele.myrods.high.DataObject;

public class DataObjectList extends ArrayList<DataObject> {
	private static final long serialVersionUID = 1L;
	
	public HashMap<String,Boolean> getOwners() {
		HashMap<String,Boolean> owners = new HashMap<String,Boolean>();
		for (DataObject obj : this) {
			owners.put(obj.getOwner(), true);
		}
		return owners;
	}
	
	public void sortByOwnerAndPath() {
		sort((Comparator<? super DataObject>) new Comparator<DataObject>() {
		    @Override
		    public int compare(DataObject a, DataObject b) {
		    	int result = a.getOwner().compareTo(b.getOwner());
		        if (result != 0) return result;
		        // sort collName separate from dataName to group data objects belonging to the same (sub)collection.
		        result = a.collName.compareTo(b.collName);
		        if (result != 0) return result;
		        return a.dataName.compareTo(b.dataName);
		    }
		});
	}
	
	public void sortByPath() {
		sort((Comparator<? super DataObject>) new Comparator<DataObject>() {
		    @Override
		    public int compare(DataObject a, DataObject b) {
		        // sort collName separate from dataName to group data objects belonging to the same (sub)collection.
		        int result = a.collName.compareTo(b.collName);
		        if (result != 0) return result;
		        return a.dataName.compareTo(b.dataName);
		    }
		});
	}
	
	public DataObjectList filterByOwner(String owner) {
		DataObjectList out = new DataObjectList();
		for (DataObject obj : this) {
			if (obj.getOwner().equals(owner)) {
				out.add(obj);
			}
		}
		return out;
	}
	
	public DataObjectList filterObjects(HashMap<String, Boolean> filter) {

		DataObjectList out = new DataObjectList();
		for (DataObject obj : this) {
			if (!filter.containsKey(obj.getPath())) {
				out.add(obj);
			}
		}
		return out;
	}
	
	
}
