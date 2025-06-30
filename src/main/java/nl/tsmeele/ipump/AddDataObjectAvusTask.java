package nl.tsmeele.ipump;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import nl.tsmeele.log.Log;
import nl.tsmeele.myrods.api.KeyValPair;
import nl.tsmeele.myrods.api.Kw;
import nl.tsmeele.myrods.api.ModAVUMetadataInp;
import nl.tsmeele.myrods.high.AVU;
import nl.tsmeele.myrods.high.DataObject;
import nl.tsmeele.myrods.high.IrodsUser;

public class AddDataObjectAvusTask extends Task {
	private DataObject dataObj;

	public AddDataObjectAvusTask(IrodsUser clientUser, boolean runAsAgent, String precondition, DataObject dataObj) {
		super(clientUser, runAsAgent, precondition);
		this.dataObj = dataObj;
	}

	@Override
	public Boolean call() throws Exception {
		Log.debug("TASK ADD AVUs TO DATA OBJECT " +  dataObj.getPath());
		
		// make sure this task runs as rodsadmin
		if (runAsAgent) {
			Log.debug("Resubmitting task AddDataObjectAvus as rodsadmin for data object " + dataObj.getPath());
			rescheduleTaskAsAdmin();
			return null;
		}

		// find all AVUs on source object
		List<AVU> avus = context.source.getAvus("-d", dataObj.getPath(), true);
		
		if (isVaultSpace(dataObj.getPath())) {
			// VAULT METADATA
			// - filter out all metadata
			// NB: Yoda will add metadata on any 'yoda-metadata.json' after the data object is transferred
			avus = new ArrayList<AVU>();
		} else {
			// RESEARCH METADATA
			// - filter out attributes with name like "org_%"
			// - keep all other metadata as-is
			avus = avus.stream().filter(a->!a.name.startsWith("org_")).collect(Collectors.toList());

		}
		
		String destPath = context.destCollectionPath(dataObj) + "/" + dataObj.dataName;
		KeyValPair options = new KeyValPair();
		options.put(Kw.ADMIN_KW, "");
		int avuError = 0;
		for (AVU avu : avus) {
			Log.debug("AVU to be set on object " + avu.toString());
			List<String> args = new ArrayList<String>();
			args.add("add");
			args.add("-d");
			args.add(destPath);
			args.add(avu.name);
			args.add(avu.value);
			args.add(avu.units);
			ModAVUMetadataInp meta = new ModAVUMetadataInp(args, options);
			context.dest.rcModAVUMetadata(meta);
			// try add the avu to target object, error -806000 = "CAT_SQL_ERR" indicates avu already present
			if (context.dest.error && context.dest.intInfo != -806000) {
				avuError = context.dest.intInfo;
			}
		}
		
		if (avuError != 0) {
			Log.error("Unable to add one or more AVU's to " + destPath + " iRODS error = " + avuError);
			return false;
		}
		
		// unblock queued tasks that have this object as precondition
		context.scheduler.unblock(Task.AVU_ADDED + dataObj.getPath());
		return null;
	}

	private static boolean isVaultSpace(String path) {
		String[] components = path.split("/");
		if (components.length >= 4 && components[3].startsWith("vault-")) {
			return true;
		}
		return false;
	}

	
}
