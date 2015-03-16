package edu.ucsb.cs.mdcc.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.ucsb.cs.mdcc.MDCCException;
import edu.ucsb.cs.mdcc.dao.Loader;

public class TPCCConfiguration {
	
    private static final Log log = LogFactory.getLog(TPCCConfiguration.class);
    	
	private int num_warehouse = 1;
	private int time_seconds = 60;
	private int backoff_times = 100;
	private int backoff_base = 0;
	private boolean flag_output = false;
	private String logconfigfile = "conf/log4j-server.properties";
	private String outputdir = "output";
	public int TermMultiplication = 10;
	public String outputFile = "result";
	
	private static volatile TPCCConfiguration config = null;
	
	private TPCCConfiguration(Properties properties) {
		num_warehouse = Integer.parseInt(properties.getProperty("warehouse", "1"));
		time_seconds = Integer.parseInt(properties.getProperty("runtime", "60"));
        backoff_times = Integer.parseInt(properties.getProperty("backofftimes", "100"));
        backoff_base = Integer.parseInt(properties.getProperty("backoffbase", "0"));
        TermMultiplication = Integer.parseInt(properties.getProperty("terminaltimes", "10"));
        flag_output = Integer.parseInt(properties.getProperty("flagoutput", "0"))==0?false:true;
        logconfigfile = properties.getProperty("logconfigfile", "conf/log4j-server.properties");
        outputdir = properties.getProperty("outputdir", "output");
        outputFile = properties.getProperty("outputfile", "result");
    }

	public String getResultDir(){
		return this.outputdir;
	}
	
	public String getLogConfigFilePath(){
		return this.logconfigfile;
	}
	
	
	public int getExpectedDbSize(){
		return num_warehouse*Loader.DIST_PER_WARE*(Loader.CUST_PER_DIST+Loader.ORD_PER_DIST)+Loader.MAXITEMS;
	}
	
	public int getExpectedTbleNum(){
		return 10;
	}
	
	public boolean getFlagOutput(){
		return this.flag_output;
	}
	
	public int getBackoffTimes(){
		return this.backoff_times;
	}
	
	public int getBackoffBase(){
		return this.backoff_base;
	}
	public static TPCCConfiguration getConfiguration() {
		synchronized (TPCCConfiguration.class) {
            if (config == null) {
                String configPath = System.getProperty("mdcc.config.dir", "conf");
                Properties props = new Properties();
                File configFile = new File(configPath, "tpcc.properties");
                try {
                    props.load(new FileInputStream(configFile));
                    config = new TPCCConfiguration(props);
                } catch (IOException e) {
                    String msg = "Error loading TPCC configuration from: " + configFile.getPath();
                    log.error(msg, e);
                    throw new MDCCException(msg, e);
                }
            }
        }
		return config;
	}

	public int getNum_warehouse() {
		return num_warehouse;
	}

	public void setNum_warehouse(int num_warehouse) {
		this.num_warehouse = num_warehouse;
	}

	public int getTime_seconds() {
		return time_seconds;
	}

	public void setTime_seconds(int time_seconds) {
		this.time_seconds = time_seconds;
	}

	public String getOutputFile() {
		return outputFile;
	}

	
	
	
	
}
