package nl.tsmeele.ipump;

import java.util.concurrent.Callable;

import nl.tsmeele.myrods.high.IrodsUser;

public abstract class Task implements Callable<Boolean> {
	// literals that may be used as prefix to an objPath to further qualify a precondition
	public static final String ADMIN_HAS_ACCESS = "ADMIN:";
	public static final String AVU_ADDED = "AVU:";
	public static final String REPUBLISHED = "PUB:";
 
	// source-side preconditions
	public IrodsUser clientUser = null;	
	public boolean runAsAgent = false;
	public String taskPrecondition = null;	// is the precondition
	
	// context received from TaskRunner
	protected TaskContext context = null;

		

	public Task(IrodsUser clientUser, boolean runAsAgent, String objPath) {
		this.clientUser = clientUser;
		this.taskPrecondition = objPath;
		this.runAsAgent = runAsAgent;
		}

	public void setContext(TaskContext context) {
		this.context = context;
	}

	public void rescheduleTaskAsAdmin() {
		runAsAgent = false;
		clientUser = new IrodsUser(context.ctx.sUserName, context.ctx.sZone);
		context.scheduler.addBlockedTask(this);
		context.scheduler.unblock(taskPrecondition);
	}
	
	public String toString() {
		return this.getClass().getSimpleName() + "(" + clientUser.nameAndZone() + ", " + (runAsAgent ? "clientuser" : "rodsadmin") + ", " + taskPrecondition + ")";   
	}



	
}
