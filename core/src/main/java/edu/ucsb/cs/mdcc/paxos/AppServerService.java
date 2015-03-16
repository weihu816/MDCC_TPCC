package edu.ucsb.cs.mdcc.paxos;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.Result;

public interface AppServerService {

	public Result read(String key);
	public Map<String, Result> read(String table, String column, List<String> columns); 
    public List<Map<String, Result>> read(String table, String key_prefix, List<String> columns, String constraintColumn, String constraintValue, String orderColumn, boolean isAssending);
    public List<Result> read(String table, String key_prefix, String projectionColumn, String constraintColumn, int lowerBound, int upperBound);
    public int read(String table, String key_prefix, String constraintColumn, int lowerBound, int upperBound);
	public boolean commit(String transactionId, Collection<Option> options);
    public void stop();

}
