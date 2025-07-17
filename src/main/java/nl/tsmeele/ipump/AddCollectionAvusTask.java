package nl.tsmeele.ipump;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import nl.tsmeele.log.Log;
import nl.tsmeele.myrods.api.KeyValPair;
import nl.tsmeele.myrods.api.Kw;
import nl.tsmeele.myrods.api.ModAVUMetadataInp;
import nl.tsmeele.myrods.high.AVU;
import nl.tsmeele.myrods.high.Collection;
import nl.tsmeele.myrods.high.IrodsUser;

public class AddCollectionAvusTask extends nl.tsmeele.ipump.Task {
	private Collection coll;

	public AddCollectionAvusTask(IrodsUser clientUser, boolean runAsAgent, String precondition, Collection coll) {
		super(clientUser, runAsAgent, precondition);
		this.coll = coll;
	}

	@Override
	public Boolean call() throws Exception {
		Log.debug("TASK ADD AVUs FOR COLL " + coll.collName);
		
		// make sure this task runs as rodsadmin
		if (runAsAgent) {
			Log.debug("Resubmitting task AddCollectionAvus as rodsadmin for collection " + coll.getPath());
			rescheduleTaskAsAdmin();
			return null;
		}
		
		List<AVU> avus = context.source.getAvus("-C", coll.getPath(), true);

		if (isVaultSpace(coll.getPath())) {
			// VAULT METADATA
			// - see method for filtering and transformation rules
			avus = transformVaultMetadata(avus);
			
		} else {
			// RESEARCH METADATA
			// - keep metadata  with name = 'org_action_log'
			//   NB: org_status should not be copied, despite SECURED or REJECTED, as this
			//       would violate Yoda status transitions policy.  Instead we copy as pristine status.
			// - filter out other attributes with name like "org_%"
			// - keep all other metadata as-is
			avus = avus.stream().filter(
					a->!(a.name.startsWith("org_") && !a.name.equals("org_action_log"))
					).collect(Collectors.toList());
		}
		
		
		String destPath = context.destCollectionPath(coll);
		KeyValPair options = new KeyValPair();
		options.put(Kw.ADMIN_KW, "");
		int avuError = 0;
		for (AVU avu : avus) {
			Log.debug("Collection AVU to be added: " + avu.toString());
			List<String> args = new ArrayList<String>();
			if (avu.name.equals("org_status") || avu.name.equals("org_vault_status")) {
				// Yoda policy requires single value with these AVU's, refuses 'add' operation
				args.add("set");
			} else {
				args.add("add");
			}
			args.add("-C");
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
		
		context.scheduler.unblock(Task.AVU_ADDED + coll.getPath());
		return null;
	}
	
	private static List<AVU> transformVaultMetadata(List<AVU> inp) {
		List<AVU> out = new ArrayList<AVU>();
		for (AVU avu : inp) {
			String name = avu.name;
			// copy as-is the below AVUs like "org_%":
			if (name.startsWith("org_publication") ||
				name.equals("org_action_log") ||
				name.equals("org_license_uri") ||
				// see YDA-6409 finding
				// value of org_data_package_reference is a UUID
				name.equals("org_data_package_reference") ||
				name.equals("org_vault_status")) {
				out.add(avu);
				continue;
			}
			// filter out all other AVUs like "org_%"
			if (name.startsWith("org_")) {
				continue;
			}
			// filter out all AVUs like "usr_%"  (= obsolete Yoda metadata)
			if (name.startsWith("usr_")) {
				continue;
			}
			// copy as-is any other avu
			out.add(avu);
		}
		return out;
	}	

	private static boolean isVaultSpace(String path) {
		String[] components = path.split("/");
		if (components.length >= 4 && components[3].startsWith("vault-")) {
			return true;
		}
		return false;
	}
	
}
