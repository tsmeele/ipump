package nl.tsmeele.ipump;

import java.util.HashMap;
import java.util.Map;

import nl.tsmeele.myrods.high.ConfigReader;
import nl.tsmeele.myrods.high.IrodsObject;
import nl.tsmeele.myrods.plumbing.MyRodsException;

public class PumpContext {
	public static final String PROGRAM_NAME = "ipump2";
	private static final String[] REQUIRED_KEYWORDS = {
			"source_host","source_port","source_username","source_zone","source_password", "source_auth_scheme",
            "destination_host", "destination_port", "destination_username", "destination_zone", 
            "destination_password", "destination_auth_scheme"};
	private static final String CONFIG_FILE = PROGRAM_NAME + ".ini";
	private static final String LOG_FILE = PROGRAM_NAME + ".log";
	
	// commandline info that can be queried after processing:
	public HashMap<String,String> options = new HashMap<String,String>();
	public String sourceObjPath = null;
	public String destinationCollPath = null;
	public String sHost, sUserName, sZone, sPassword;
	public String dHost, dUserName, dZone, dPassword;
	public int sPort, dPort;
	public boolean sAuthPam, dAuthPam;
	
	public boolean verbose = false;
	public boolean debug = false;
	public boolean resume = false;
	public String resumeFile = LOG_FILE;
	public String logFile = LOG_FILE;
	public int threads = 1;
	public boolean usage = false;
	
	// information added during session by PumpMain, after connections have been established
	public String sourceLocalZone = null;
	public String destLocalZone = null;
	public IrodsObject sourceObject = null;
	public IrodsObject destObject = null;
	public LogFile log = null;
	
	
	public void processArgs(String[] args) throws MyRodsException {
		String configFile = CONFIG_FILE;
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
					debug = true;
					break;
				}
				case "-v":
				case "-verbose": {
					verbose = true;
					break;
				}
				case "-c":
				case "-config": {
					if (argIndex < args.length + 1) {
						argIndex++;
						configFile = args[argIndex];
					}
					break;
				}
				case "-resume": {
					if (argIndex < args.length + 1) {
						argIndex++;
						resumeFile = args[argIndex];
						resume = true;
					}
					break;
				}
				case "-l":
				case "-log": {
					if (argIndex < args.length + 1) {
						argIndex++;
						logFile = args[argIndex];
					}
					break;
				}
				case "-t":
				case "-threads": {
					if (argIndex < args.length + 1) {
						argIndex++;
						try {
							threads = Integer.valueOf(args[argIndex]);
						} catch (NumberFormatException e) { 
							/* keep threads 1 in case of parse error */ 
						}
					}
					if (threads < 1) threads = 1;
					break;
				}
				// add new options above this line
				case "-h":
				case "-help":
				case "-?":	
					// an unknown option will trigger the help option
				default: 
					usage =true;
			}
			argIndex++;
		}
		
		// process the remaining, non-option, arguments
		if (argIndex < args.length) {
			sourceObjPath = args[argIndex++];
		}
		if (argIndex < args.length) {
			destinationCollPath = args[argIndex++];
		}
		
		// read and process configuration information
		ConfigReader configReader = new ConfigReader();
		Map<String,String> config = configReader.readConfig(configFile, REQUIRED_KEYWORDS);
		if (config == null) {
			throw new MyRodsException("Missing configuration file: " + configFile);
		}
		sHost 		= config.get("source_host");
		sPort 		= Integer.parseInt(config.get("source_port"));
		sUserName 	= config.get("source_username");
		sZone 		= config.get("source_zone");
		sPassword 	= config.get("source_password");
		sAuthPam 	= config.get("source_auth_scheme").toLowerCase().startsWith("pam");
		dHost 		= config.get("destination_host");
		dPort 		= Integer.parseInt(config.get("destination_port"));
		dUserName 	= config.get("destination_username");
		dZone 		= config.get("destination_zone");
		dPassword 	= config.get("destination_password");
		dAuthPam 	= config.get("destination_auth_scheme").toLowerCase().startsWith("pam");
	}
	
	public String usage() {
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
	
	private String printKeywords(String[] keywords) {
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
			"verbose / debug / usage      = " + verbose + " / " + debug + " / " + usage + "\n" +
			"resume : file                = " + resume + " : " + resumeFile + "\n" +
			"logfile                      = " + logFile + "\n" +
			"threads                      = " + threads + "\n" +
			"sHost : sPort                = " + sHost + " : " + sPort + "\n" +
			"sUsername # sZone (sAuthPam) = " + sUserName + " # " + sZone + " (" + sAuthPam + ")\n" +
			"sPassword                    = " + (sPassword == null || sPassword.equals("")? "null" : "*redacted*") + "\n" +
			"dHost : dPort                = " + dHost + " : " + dPort + "\n" +
			"dUsername # dZone (dAuthPam) = " + dUserName + " # " + dZone + " (" + dAuthPam + ")\n" +
			"dPassword                    = " + (dPassword == null || dPassword.equals("")? "null" : "*redacted*") + "\n" +
			"sourceObject                 = " + sourceObjPath + "\n" +
			"destinationCollection        = " + destinationCollPath + "\n";
	}
	
}
