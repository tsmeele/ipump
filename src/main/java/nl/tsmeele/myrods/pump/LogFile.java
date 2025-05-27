package nl.tsmeele.myrods.pump;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class LogFile {
	private String logfilePath;
	private BufferedWriter logfile = null;
	
	public LogFile(String logfilePath) {
		openAppend(logfilePath);
	}
	
	public void openAppend(String logfilePath) {
		if (logfile != null) return;
		this.logfilePath = logfilePath;
		try {
			logfile = new BufferedWriter(new FileWriter(logfilePath, true));
		} catch (IOException e) {
			// logfile is null upon any error
		}
	}
	
	public void close() {
		if (logfile != null) {
			try {
				logfile.close();
			} catch (IOException e) {
				// we ignore close errors
			}
		}
	}
	
	public synchronized void LogDone(String path) throws IOException {
		openAppend(logfilePath);
		logfile.write("OK " + path + "\n");
		logfile.flush();
	}
	
	public synchronized void LogError(String path, String error) throws IOException {
		openAppend(logfilePath);
		logfile.write("ERROR " + path + " : " + error + "\n");
		logfile.flush();
	}

	public static HashMap<String,Boolean> slurpCompletedObjects(String path) throws IOException  {
		HashMap<String,Boolean> out = new HashMap<String,Boolean>();
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(path));
		} catch (FileNotFoundException e) {
			return null;
		}
		try {
			String line = br.readLine();
			while (line != null) {
				if (line.startsWith("OK ")) {
					String objPath = line.substring(3);
					out.put(objPath,true);
				}
				line = br.readLine();
			}
		} finally {
			br.close();
		}
		return out;
	}
	
}
