package edu.ucsb.cs.mdcc.paxos;

import edu.ucsb.cs.mdcc.messaging.ReadValue;

import java.util.List;
import java.util.Map;

public interface AgentService {
    
    public boolean onPrepare(Prepare prepare);
    
    public boolean onAccept(Accept accept);
    
    public boolean runClassic(String transaction, String object,
                              long oldVersion, byte[] value);
    
    public void onDecide(String transaction, boolean commit);
    
    public ReadValue onRead(String object);
    
    // WeiHu
    public Map<String, ReadValue> onRead(String table, String key, List<String> columns);

 	public List<Map<String, ReadValue>> onRead(String table, String key_prefix,
			List<String> columns, String constraintColumn,
			String constraintValue, String orderColumn, boolean isAssending);

 	public List<ReadValue> onRead(String table, String key_prefix, String projectionColumn, String constraintColumn, long lowerBound,
			long upperBound);
 	
 	public Integer onRead(String table, String key_prefix, String constraintColumn,
			long lowerBound, long upperBound);
 	
 	// WeiHu
    
    public Map<String, ReadValue> onRecover(Map<String, Long> versions);
}