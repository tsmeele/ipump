package nl.tsmeele.ipump;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import nl.tsmeele.log.Log;

/**
 * TaskScheduler schedules and executes tasks on behalf of an IrodsUser.
 * It maintains two queue structures:
 * 
 * 1. Blocked : 
 * Tasks on this Map structure are queued per IrodsObject (or other resource). 
 * Queued tasks require the IrodsObject to exist before they can be executed
 * 
 * 2. Runnable:
 * Tasks on this Map structure are queued per IrodsUser.
 * Queued tasks require an authenticated session (on behalf of the IrodsUser) 
 * to the source and destination Irods servers. 
 * 
 * The scheduler will submit the execution of a "run" for an IrodsUser, if
 * at least one task for that user is present on the Runnable queue.
 * A TaskRunner object is responsible for managing the run.
 * The run will sequentially process queued tasks for that user, until the 
 * queue for that user is exhausted. 
 * As long as the run executes, it will pick up any new tasks that are meanwhile
 * added to the runnable queue for that user.
 * At the start of each task execution, the TaskRunner ensures that an authenticated
 * connection to source and destination iRODS servers has been established.
 * It disconnects those connections at the end of a run. 
 * 
 * The scheduler is capable of running multiple runs in parallel, hence service
 * multiple IrodsUser. Each run executes in a separate thread. 
 * 
 * The scheduler stops when all scheduled runs have completed and the 
 * Runnable queue is completely empty.
 *
 * The scheduler itself does not unblock tasks to make them runnable. This
 * can be done by executing tasks. For instance, a task that creates a
 * collection may, after the collection has been created, decide to unblock 
 * all tasks that wait for this collection object to exist.
 *  
 * @author ton
 *
 */
public class TaskScheduler {
	private final int MAXTHREADS = 8;
	private int clientThreads = 2; // default
	private PumpContext ctx;
	private HashMap<String, Queue<Task>> blocked = new HashMap<String, Queue<Task>>();
	private HashMap<String, Queue<Task>> runnable = new HashMap<String, Queue<Task>>();
	
	public TaskScheduler(PumpContext ctx, int clientThreads) {
		if (clientThreads > 0 && clientThreads <= MAXTHREADS) {
			this.clientThreads = clientThreads;
		}
		this.ctx = ctx;
	}
	
	public int countBlockedObjects() {
		return blocked.keySet().size();
	}
	
	/**
	 * RunTasks will keep scheduling tasks to run until the Runnable queue is exhausted.
	 * 
	 */
	public void runTasks() {
		ExecutorService executor = Executors.newFixedThreadPool(clientThreads);
		HashMap<String,Future<Boolean>> scheduled = new HashMap<String,Future<Boolean>>();
		
		while (!runnable.isEmpty() || !scheduled.isEmpty()) {
			// schedule a runner for each IrodsUser that has runnable tasks
			for (String clientUser : runnable.keySet()) {
				// only schedule a new runner if not active already
				if (!scheduled.containsKey(clientUser)) {
					Future<Boolean> runner = executor.submit(new TaskRunner(ctx, this, clientUser));
					scheduled.put(clientUser, runner);
				}
			}
			// wait a little, allow some runners to complete
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) { }
			
			// account for completed runners
			for (Iterator<HashMap.Entry<String,Future<Boolean>>> it = scheduled.entrySet().iterator(); it.hasNext();) {
				Map.Entry<String,Future<Boolean>> entry = it.next();
				// String clientUser = entry.getKey();
				Future<Boolean> runner = entry.getValue();
				if (runner.isDone()) {
					it.remove();
				}
			}
		} 
		
		// All runners are done and our runnable queue is exhausted...we're done
		executor.shutdown();
		Log.debug("Scheduler shutdown");
	}
	
	public synchronized void addBlockedTask(Task task) {
		Queue<Task> tasks = blocked.get(task.taskPrecondition);
		if (tasks == null) {
			tasks = new ConcurrentLinkedQueue<Task>();
		}
		tasks.add(task);
		blocked.put(task.taskPrecondition, tasks);
	}
	
	public synchronized void unblock(String objPath) {
		Queue<Task> tasks = blocked.remove(objPath);
		if (tasks == null) return;
		Task task = tasks.poll();
		while (task != null) {
			addRunnableTask(task);
			task = tasks.poll();
		}
	}
	
	private synchronized void addRunnableTask(Task task) {
		Queue<Task> tasks = runnable.get(task.clientUser.nameAndZone());
		if (tasks == null) {
			tasks = new ConcurrentLinkedQueue<Task>();
		}
		tasks.add(task);
		runnable.put(task.clientUser.nameAndZone(), tasks);
	}
	
	public synchronized Task pollRunnableTask(String clientUser) {
		Queue<Task> tasks = runnable.get(clientUser);
		// any task available?
		if (tasks == null) return null;
		// poll first available task, cleanup queue if it is the last task
		Task task = tasks.poll();
		if (tasks.isEmpty()) {
			runnable.remove(clientUser);
		} else {
			runnable.put(clientUser, tasks);
		}
		return task;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Blocked tasks:\n");
		for (String key : blocked.keySet()) {
			sb.append(key + " : " + blocked.get(key).toString() + "\n");
		}
		sb.append("\nUnblocked tasks:\n");
		for (String key : runnable.keySet()) {
			sb.append(key + " : " + runnable.get(key).toString() + "\n");
		}
		return sb.toString();
	}
	
}
