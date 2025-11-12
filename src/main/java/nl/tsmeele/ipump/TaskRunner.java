package nl.tsmeele.ipump;

import java.util.concurrent.Callable;

import nl.tsmeele.log.Log;


public class TaskRunner implements Callable<Boolean> {
	/* 
	 * Long-running connections may lead to an increase in memory usage at iRODS Agents (in a server),
	 * caused e.g. by memory leaks in custom rules or microservices.
	 * 
	 * To avoid excessive resource use issues, we limit connection duration by forcing a reconnect
	 * after X executed tasks, configured in MAX_TASKS_PER_CONNECTION.
	 * This feature is turned off by setting the constant to a value 0.
	 */
	private final int MAX_TASKS_PER_CONNECTION = 3000;
	private TaskScheduler scheduler;
	private String clientUser;
	private PumpContext ctx;
	private TaskContext context = null;
	
	
	public TaskRunner(PumpContext ctx, TaskScheduler scheduler, String clientUser) {
		this.ctx = ctx;
		this.scheduler = scheduler;
		this.clientUser = clientUser;
	}

	@Override
	public Boolean call() throws Exception {
		context = new TaskContext(ctx, scheduler);
		
		Task nextTask = scheduler.pollRunnableTask(clientUser);
		int seq = 0;
		while (nextTask != null) {
			Task thisTask = nextTask;
			nextTask = scheduler.pollRunnableTask(clientUser);
			thisTask.setContext(context);
			Log.debug("Running task" + seq + " " + thisTask.taskPrecondition + " for user " + clientUser + " as " + (thisTask.runAsAgent ? "clientuser" : "rodsadmin"));

			// check if max tasks per connection is reached
			seq++;
			if (MAX_TASKS_PER_CONNECTION > 0 && seq >= MAX_TASKS_PER_CONNECTION) {
				// force a reconnect
				Log.debug("Max tasks per connection reached, reconnecting");
				context.disconnect();
				seq = 0;
			}
			
			// establish server connections for task 
			if (thisTask.runAsAgent && !context.loginAsClientUser(thisTask.clientUser)) {
				Log.error("Task unable to login as " + thisTask.clientUser.nameAndZone() + " on source and/or destination");
				continue;
			} 
			if (!thisTask.runAsAgent && !context.loginAsAdmin()) {
				Log.error("Task unable to login as rodsadmin on source and/or destination");
				continue;
			}
			
			// execute the task
			try {
				thisTask.call();
			} catch (Exception e) { 
				Log.debug("Task has thrown: " + e.getMessage());
				// establish a clean starting point for next task
				context.disconnect();
				seq = 0;
			}
		}
		// make sure any remaining connections are cleaned up after all tasks are done
		context.disconnect();
		return true;
	}


}
