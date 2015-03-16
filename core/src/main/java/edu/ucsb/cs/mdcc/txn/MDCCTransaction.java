package edu.ucsb.cs.mdcc.txn;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.Result;
import edu.ucsb.cs.mdcc.paxos.AppServerService;
import edu.ucsb.cs.mdcc.paxos.Transaction;
import edu.ucsb.cs.mdcc.paxos.TransactionException;

public class MDCCTransaction extends Transaction {

    private AppServerService appServer;

    public MDCCTransaction(AppServerService appServer) {
        super();
        this.appServer = appServer;
    }

    protected Result doRead(String key) {
        return appServer.read(key);
    }

    @Override
    protected void doCommit(String transactionId,
                            Collection<Option> options) throws TransactionException {
        boolean success = appServer.commit(transactionId, options);
        if (!success) {
            throw new TransactionException("Failed to commit txn: " + transactionId);
        }
    }
    
    // TODO [wei] 
 	public Map<String, Result> doRead(String table, String column, List<String> columns) {
 		return appServer.read(table, column, columns);
 	}

 	public List<Map<String, Result>> doRead(String table, String key_prefix,
 			List<String> columns, String constraintColumn, String constraintValue,
 			String orderColumn, boolean isAssending) {
 		return appServer.read(table, key_prefix, columns, constraintColumn, constraintValue, orderColumn, isAssending);
 	}

 	public List<Result> doRead(String table, String key_prefix,
 			String projectionColumn, String constraintColumn, int lowerBound, int upperBound) {
 		return appServer.read(table, key_prefix, projectionColumn, constraintColumn, lowerBound, upperBound);
 	}
 	
 	public Integer doRead(String table, String key_prefix, String constraintColumn, int lowerBound, int upperBound) {
 		return appServer.read(table, key_prefix, constraintColumn, lowerBound, upperBound);
 	}

 	public boolean write(String table, String column, List<String> columns, List<String> values, int action) {
 		if (action == 0 || action == 1) {
 			for (int i = 0; i < columns.size(); i++) {
 				String name = columns.get(i);
 				String data = values.get(i);
 				try {
 					StringBuilder stringBuilder = new StringBuilder();
 					stringBuilder.append(table);
 					stringBuilder.append(":");
 					stringBuilder.append(name);
 					stringBuilder.append(":");
 					stringBuilder.append(column);
 					String key = stringBuilder.toString();
 					super.write(key, data.getBytes().clone());
 				} catch (TransactionException e) {
 					e.printStackTrace();
 				}
 			}
 		} else if (action == 2) {
 			for (int i = 0; i < columns.size(); i++) {
 				String name = columns.get(i);
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append(table);
				stringBuilder.append(":");
				stringBuilder.append(name);
				stringBuilder.append(":");
				stringBuilder.append(column);
				String key = stringBuilder.toString();
 				try {
 					super.delete(key);
 				} catch (TransactionException e) {
 					e.printStackTrace();
 				}
 			}
 		}
 		
 		return true;
 	}
}
