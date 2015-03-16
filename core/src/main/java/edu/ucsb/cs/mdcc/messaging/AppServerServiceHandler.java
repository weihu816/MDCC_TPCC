package edu.ucsb.cs.mdcc.messaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.TException;

import edu.ucsb.cs.mdcc.Result;
import edu.ucsb.cs.mdcc.messaging.MDCCAppServerService.Iface;
import edu.ucsb.cs.mdcc.paxos.AppServerService;

public class AppServerServiceHandler implements Iface {
	
	private AppServerService appServer;

	public AppServerServiceHandler(AppServerService appServer) {
		this.appServer = appServer;
	}

	public boolean ping() throws TException {
		return true;
	}

	public ReadValue read(String key) throws TException {
		return toThriftReadValue(appServer.read(key));
	}

	public boolean commit(String transactionId, List<Option> options)
			throws TException {
		Collection<edu.ucsb.cs.mdcc.Option> mOptions = new ArrayList<edu.ucsb.cs.mdcc.Option>(options.size());
		for(Option o : options) {
			mOptions.add(toPaxosOption(o));
		}
		return appServer.commit(transactionId, mOptions);
	}
	
	private static ReadValue toThriftReadValue(Result r) {
		//This does not return the correct classicEndVersion, but it does allow the client to determine classic mode
		long classicEndVersion = (r.isClassic() ? r.getVersion() : r.getVersion() - 1);
		//TODO r.getKey()
		return new ReadValue(r.getKey(), r.getVersion(), classicEndVersion, ByteBuffer.wrap(r.getValue()));
	}
	
	private static edu.ucsb.cs.mdcc.Option toPaxosOption(Option o) {
		return new edu.ucsb.cs.mdcc.Option(o.getKey(), o.getValue(),
                o.getOldVersion(), o.isClassic());
	}

	//------------------------------
	@Override
	public Map<String, ReadValue> read2(String table, String column,
			List<String> columns) throws TException {
		Map<String, ReadValue> ret = new HashMap<String, ReadValue>();
		Map<String, Result> reads = appServer.read(table, column, columns);
		for (Entry<String, Result> e : reads.entrySet()) {
			Result r = e.getValue();
			long classicEndVersion = (r.isClassic() ? r.getVersion() : r.getVersion() - 1);
			//TODO r.getKey()
			ReadValue readValue = new ReadValue(r.getKey(), r.getVersion(),
					classicEndVersion, ByteBuffer.wrap(r.getValue()));
			ret.put(e.getKey(), readValue);
		}
		return ret;
	}

	@Override
	public List<Map<String, ReadValue>> read3(String table, String key_prefix,
			List<String> columns, String constraintColumn,
			String constraintValue, String orderColumn, boolean isAssending)
			throws TException {
		List<Map<String, ReadValue>> ret = new ArrayList<Map<String, ReadValue>>();
		List<Map<String, Result>> reads = appServer.read(table, key_prefix,
				columns, constraintColumn, constraintValue, orderColumn,
				isAssending);
		for (Map<String, Result> m : reads) {
			Map<String, ReadValue> ret_ = new HashMap<String, ReadValue>();
			for (Entry<String, Result> e : m.entrySet()) {
				Result r = e.getValue();
				long classicEndVersion = (r.isClassic() ? r.getVersion() : r.getVersion() - 1);
				//TODO r.getKey()
				ReadValue readValue = new ReadValue(r.getKey(), r.getVersion(),
						classicEndVersion, ByteBuffer.wrap(r.getValue()));
				ret_.put(e.getKey(), readValue);
			}
			ret.add(ret_);
		}
		return ret;
	}

	@Override
	public List<ReadValue> read4(String table, String key_prefix,
			String projectionColumn, String constraintColumn, int lowerBound,
			int upperBound) throws TException {
		List<ReadValue> ret = new ArrayList<ReadValue>();
		List<Result> reads = appServer.read(table, key_prefix,
				projectionColumn, constraintColumn, lowerBound, upperBound);
		for (Result r : reads) {
			long classicEndVersion = (r.isClassic() ? r.getVersion() : r.getVersion() - 1);
			//TODO r.getKey()
			ReadValue readValue = new ReadValue(r.getKey(), r.getVersion(),
					classicEndVersion, ByteBuffer.wrap(r.getValue()));
			ret.add(readValue);
		}
		return ret;
	}

	@Override
	public int read5(String table, String key_prefix, String constraintColumn,
			int lowerBound, int upperBound) throws TException {
		return appServer.read(table, key_prefix, constraintColumn, lowerBound, upperBound);
	}
	//------------------------------

}
