package nl.tsmeele.ipump;

import java.io.IOException;

import nl.tsmeele.log.Log;
import nl.tsmeele.myrods.high.Hirods;
import nl.tsmeele.myrods.high.IrodsObject;
import nl.tsmeele.myrods.high.IrodsUser;

/**
 * TaskContext manages Irods connections that can sustain across tasks
 * @author ton
 *
 */
public class TaskContext {
	public PumpContext ctx;
	public TaskScheduler scheduler;

	
	public Hirods source;
	public Hirods dest;
	String sourceRoot;
	String destRoot;
	private boolean loggedIn = false;
	private boolean runAsAgent = false;
	
	
	public TaskContext(PumpContext ctx, TaskScheduler scheduler) {
		this.ctx = ctx;
		this.scheduler = scheduler;
		source = new Hirods(ctx.sHost,ctx.sPort);
		dest = new Hirods(ctx.dHost,ctx.dPort);
		// get path of the top of the collection tree at source and destination
		if (ctx.sourceObject.isCollection()) {
			sourceRoot = ctx.sourceObject.getPath();
		} else {
			sourceRoot = ctx.sourceObject.getParentPath();
		}
		destRoot = ctx.destObject.getPath();
	}
	
	public String destCollectionPath(IrodsObject sourceObject) {
		String sourcePath;
		if (sourceObject.isCollection()) {
			sourcePath = sourceObject.getPath();
		} else {
			sourcePath = sourceObject.getParentPath();
		}
		String suffix = sourcePath.substring(sourceRoot.length());
		if (suffix.length() > 0) {
			return destRoot + suffix;
		}
		return destRoot;
	}
	
	
	public void disconnect() {
		// we will ignore any errors or exceptions
		Log.debug("logging out");
		try {
			source.rcDisconnect();
		} catch (IOException e) {
		}
		try {
			dest.rcDisconnect();
		} catch (IOException e) {
		}
		loggedIn = false;
	}
	
	public boolean loginAsAdmin() {
		// already logged in?
		if (!runAsAgent && loggedIn) return true;
		Log.debug("logging in as admin");
		// attempt to login as Admin
		boolean sourceLogin = login(source, ctx.sUserName, ctx.sZone, ctx.sPassword, ctx.sAuthPam, ctx.sUserName, ctx.sZone);
		boolean destLogin = login(dest, ctx.dUserName,ctx.dZone, ctx.dPassword, ctx.dAuthPam, ctx.dUserName, ctx.dZone);
		if (sourceLogin && destLogin) {
			loggedIn = true;
			runAsAgent = false;
			return true;
		}
		disconnect();
		return false;
	}
	
	public boolean loginAsClientUser(IrodsUser clientUser) {
		// already logged in?
		if (runAsAgent && loggedIn) return true;
		Log.debug("logging in on behalf of clientUser (" + clientUser.nameAndZone() + ")");
		// attempt to login on behalf of clientUser (with zone transformed to localZone at both ends)
		boolean sourceLogin = login(source, ctx.sUserName, ctx.sZone, ctx.sPassword, ctx.sAuthPam, clientUser.name, ctx.sourceLocalZone);
		boolean destLogin = login(dest, ctx.dUserName,ctx.dZone, ctx.dPassword, ctx.dAuthPam, clientUser.name, ctx.destLocalZone);
		if (sourceLogin && destLogin) {
			loggedIn = true;
			runAsAgent = true;
			return true;
		}
		disconnect();
		return false;
	}

	private boolean login(Hirods hirods, String proxyUser, String proxyZone, String proxyPassword, boolean authPam, String clientUser, String ClientZone) {
		boolean success = false;
		try {
			if (authPam) {
				hirods.pamLogin(proxyUser, proxyZone, proxyPassword, clientUser, ClientZone);
			} else {
				hirods.nativeLogin(proxyUser, proxyZone, proxyPassword, clientUser, ClientZone);
			}
			success = !hirods.error;
		} catch (IOException e) {
		}
		return success;
	}
	
	
}
