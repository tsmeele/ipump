package nl.tsmeele.myrods.pump;

import java.util.HashMap;
import java.util.Map;

import nl.tsmeele.myrods.high.ConfigReader;
import nl.tsmeele.myrods.plumbing.MyRodsException;

public class Context {
	private static final String PROGRAM_NAME = "ipump";
	private static final String[] REQUIRED_KEYWORDS = {
			"source_host","source_port","source_username","source_zone","source_password", "source_auth_scheme",
            "destination_host", "destination_port", "destination_username", "destination_zone", 
            "destination_password", "destination_auth_scheme"};
	private static final String CONFIG_FILE = PROGRAM_NAME + ".ini";
	private static final String LOG_FILE = PROGRAM_NAME + ".log";
	
	// commandline info that can be queried after processing:
	public HashMap<String,String> options = new HashMap<String,String>();
	public String sourceObject = null;
	public String destinationCollection = null;
	public String sHost, sUsername, sZone, sPassword;
	public String dHost, dUsername, dZone, dPassword;
	public int sPort, dPort;
	public boolean sAuthPam, dAuthPam;
	
	
	
	public void processArgs(String[] args) throws MyRodsException {
		// set defaults
		options.put("log", LOG_FILE);
		options.put("threads", "1");
		// collect and process arguments
		int argIndex = 0;
		while (argIndex < args.length) {
			String optionArg = args[argIndex].toLowerCase();
			if (!optionArg.startsWith("-")) {
				break;
			}
			switch (optionArg) {
				// debug is a hidden option 
				case "-d":
				case "-debug":	{
					options.put("debug", null);
					break;
				}
				case "-v":
				case "-verbose": {
					options.put("verbose", null);
					break;
				}
				case "-c":
				case "-config": {
					String configFile = CONFIG_FILE;
					if (argIndex < args.length + 1) {
						argIndex++;
						configFile = args[argIndex];
					}
					options.put("config", configFile);
					break;
				}
				case "-resume": {
					String logFile = LOG_FILE;
					if (argIndex < args.length + 1) {
						argIndex++;
						logFile = args[argIndex];
					}
					options.put("resume", logFile);
					break;
				}
				case "-l":
				case "-log": {
					String logFile = LOG_FILE;
					if (argIndex < args.length + 1) {
						argIndex++;
						logFile = args[argIndex];
					}
					options.put("log", logFile);
					break;
				}
				case "-t":
				case "-threads": {
					String threads = "1";
					if (argIndex < args.length + 1) {
						argIndex++;
						threads = args[argIndex];
					}
					options.put("threads", threads);
					break;
				}
				// add new options above this line
				case "-h":
				case "-help":
				case "-?":	
					// an unknown option will trigger the help option
				default: 
					options.put("help", null);
			}
			argIndex++;
		}
		
		// process the remaining, non-option, arguments
		if (argIndex < args.length) {
			sourceObject = args[argIndex++];
		}
		if (argIndex < args.length) {
			destinationCollection = args[argIndex++];
		}
		
		// read and process configuration information
		ConfigReader configReader = new ConfigReader();
		String configFile = CONFIG_FILE;
		if (options.containsKey("config")) {
			configFile = options.get("config");
		}
		Map<String,String> config = configReader.readConfig(configFile, REQUIRED_KEYWORDS);
		if (config == null) {
			throw new MyRodsException("Missing configuration file: " + configFile);
		}
		sHost 		= config.get("source_host");
		sPort 		= Integer.parseInt(config.get("source_port"));
		sUsername 	= config.get("source_username");
		sZone 		= config.get("source_zone");
		sPassword 	= config.get("source_password");
		sAuthPam 	= config.get("source_auth_scheme").toLowerCase().startsWith("pam");
		dHost 		= config.get("destination_host");
		dPort 		= Integer.parseInt(config.get("destination_port"));
		dUsername 	= config.get("destination_username");
		dZone 		= config.get("destination_zone");
		dPassword 	= config.get("destination_password");
		dAuthPam 	= config.get("destination_auth_scheme").toLowerCase().startsWith("pam");
	}
		
	
	
	public static String usage() {
		return
				"Usage: " + PROGRAM_NAME + " [-config <configfile>] <source_object> <destination_collection>\n" +
		        "<source_object> can be a data object or collection. In case of collection, all its content is recursively copied.\n" +
				"<destination_collection> must already exist.\n\n" +
		        "Options:\n" +
				"-help, -h, -?           : exit after showing this usage text.\n" +
				"-verbose, -v            : print names of processed objects.\n" +
				"-log, -l                : specify name of logfile (default is '" + LOG_FILE + "')\n" +
				"-resume <logfile>       : resume an aborted operation, using logfile from previous operation\n" +
				"-threads <#threads>, -t : specify number of parallel threads to use. Default is 1 thread.\n" +
		        "-config <configfile>    :\n" +
		        "   The configfile is a local path to a textfile with configuration key=value lines.\n" +
		        "\nConfiguration file keywords:\n" +
				printKeywords(REQUIRED_KEYWORDS) + "\n";
	}
	
	private static String printKeywords(String[] keywords) {
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (String s: keywords) {
			sb.append("  " + s);
			if (i >= 4) sb.append("\n");
			i = (i + 1) % 5; 
		}
		return sb.toString();
	}
	
	public String toString() {
		return 
			"options                      = " + options.entrySet().toString() + "\n" +
			"sHost : sPort                = " + sHost + " : " + sPort + "\n" +
			"sUsername # sZone (sAuthPam) = " + sUsername + " # " + sZone + " (" + sAuthPam + ")\n" +
			"sPassword                    = " + (sPassword == null || sPassword.equals("")? "null" : "*redacted*") + "\n" +
			"dHost : dPort                = " + dHost + " : " + dPort + "\n" +
			"dUsername # dZone (dAuthPam) = " + dUsername + " # " + dZone + " (" + dAuthPam + ")\n" +
			"dPassword                    = " + (dPassword == null || dPassword.equals("")? "null" : "*redacted*") + "\n" +
			"sourceObject                 = " + sourceObject + "\n" +
			"destinationCollection        = " + destinationCollection + "\n";
	}
	
}
