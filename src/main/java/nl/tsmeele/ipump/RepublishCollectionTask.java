package nl.tsmeele.ipump;

import java.util.List;
import java.util.stream.Collectors;

import nl.tsmeele.log.Log;
import nl.tsmeele.myrods.high.AVU;
import nl.tsmeele.myrods.high.Collection;
import nl.tsmeele.myrods.high.IrodsUser;

public class RepublishCollectionTask extends Task {
	private Collection coll;

	public RepublishCollectionTask(IrodsUser clientUser, boolean runAsAgent, String precondition, Collection coll) {
		super(clientUser, runAsAgent, precondition);
		this.coll = coll;
	}

	@Override
	public Boolean call() throws Exception {
		Log.debug("TASK REPUBLISH COLL " + coll.collName);

		// execute this task only for vault collections, otherwise flag as complete
		if (!isVaultSpace(coll.getPath())) {
			context.scheduler.unblock(Task.REPUBLISHED + coll.getPath());
			return null;
		}
		
		
		// make sure this task runs as rodsadmin
		if (runAsAgent) {
			Log.debug("Resubmitting task RepublishCollection as rodsadmin for collection " + coll.getPath());
			rescheduleTaskAsAdmin();
			return null;
		}
		
		// find out if this collection is a (de)published data package
		String destPath = context.destCollectionPath(coll);
		List<AVU> avus = context.dest.getAvus("-C", destPath, true);
		if (avus.stream().filter(a->!(a.name.equals("org_vault_status") && a.value.equals("PUBLISHED")) ).collect(Collectors.toList()).isEmpty()) {
			// published data package, signal in logfile that rule to republish must be executed
			// (to fix any zone/url refs in metadata, recreate landingpage, and update DOI at Datacite)
			// NB: Rule to call is: rule_update_publication(vault_package_coll_name, "Yes","Yes","Yes")
			context.ctx.log.logRePublish(destPath);
		}
		if (avus.stream().filter(a->!(a.name.equals("org_vault_status") && a.value.equals("DEPUBLISHED")) ).collect(Collectors.toList()).isEmpty()) {
			// yes it is, signal in logfile that a rule to redo depublication must be executed
			// (to fix any zone/url refs in metadata, recreate landingpage, and update DOI at Datacite)
			// NB: Rule to call is: ??
			context.ctx.log.logReDepublish(destPath);
		}
		
		context.scheduler.unblock(Task.REPUBLISHED + coll.getPath());
		return null;
	}
	

	
	

	private static boolean isVaultSpace(String path) {
		String[] components = path.split("/");
		if (components.length >= 3 && components[2].startsWith("vault-")) {
			return true;
		}
		return false;
	}
	
}
