package edu.ucsb.cs.mdcc.messaging;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.StackKeyedObjectPool;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import edu.ucsb.cs.mdcc.Result;
import edu.ucsb.cs.mdcc.config.Member;

public class RemoteAppServer implements edu.ucsb.cs.mdcc.paxos.AppServerService {

    private static final Log log = LogFactory.getLog(RemoteAppServer.class);
	
	private Member member;
	Map<String, Map<Integer, TTransport>> transports = new HashMap<String, Map<Integer, TTransport>>();
	
	private KeyedObjectPool<Member,TTransport> blockingPool =
            new StackKeyedObjectPool<Member, TTransport>(new ThriftConnectionPool());
	
	public RemoteAppServer(Member member) {
		this.member = member;
	}
	
	//check whether an AppServer is up and reachable
	public boolean ping() {
        String host = member.getHostName();
        int port = member.getPort();
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
            MDCCAppServerService.Client client = getClient(transport);
            return client.ping();
        } catch (TException e) {
            handleException(host, e);
            return false;
        } finally {
            //close(transport);
        }
    }

	public TTransport getTTransport(String host, int port) {
		Map<Integer, TTransport> x = transports.get(host);
		if (x == null) {
			x = new HashMap<Integer, TTransport>();
			transports.put(host, x);
		}
		TTransport y = x.get(port);
		if (y == null) {
			y = new TFramedTransport(new TSocket(host, port));
			x.put(port, y);
		}
		return y;
		/*TTransport transport = null;
		try {
			transport = blockingPool.borrowObject(member);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return transport;*/
	}
	
	public Result read(String key) {
		String host = member.getHostName();
        int port = member.getPort();
        TTransport transport = getTTransport(host, port);
        //TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
        	MDCCAppServerService.Client client = getClient(transport);
        	Result ret = toPaxosResult(key, client.read(key));
        	//close(transport);
            return ret;
        } catch (TException e) {
            handleException(host, e);
            return null;
        } finally {
            
        }
	}

	public boolean commit(String transactionId, Collection<edu.ucsb.cs.mdcc.Option> options) {
		String host = member.getHostName();
        int port = member.getPort();
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
        	MDCCAppServerService.Client client = getClient(transport);
        	List<Option> tOptions = new ArrayList<Option>(options.size());
        	for(edu.ucsb.cs.mdcc.Option o : options) {
        		tOptions.add(toThriftOption(o));
        	}
            return client.commit(transactionId, tOptions);
        } catch (TException e) {
            handleException(host, e);
            return false;
        } finally {
            //close(transport);
        }
	}

    public void stop() {

    }

    private MDCCAppServerService.Client getClient(
            TTransport transport) throws TTransportException {
    	if (!transport.isOpen()) {
    		transport.open();
    	}	
        TProtocol protocol = new TBinaryProtocol(transport);
        return new MDCCAppServerService.Client(protocol);
    }
	
	private void close(TTransport transport) {
        if (transport.isOpen()) {
            transport.close();
        }
    }
	
	private void handleException(String target, Exception e) {
        String msg = "Error contacting the remote member: " + target;
        log.warn(msg, e);
    }
	
	private static Result toPaxosResult(String key, ReadValue r) {
		return new Result(key, r.getValue(), r.getVersion(),
                r.classicEndVersion >= r.getVersion());
	}
	
	private static Option toThriftOption(edu.ucsb.cs.mdcc.Option o) {
		return new Option(o.getKey(), o.getOldVersion(),
                ByteBuffer.wrap(o.getValue()), o.isClassic());
	}

	//------------------------------
	@Override
	public Map<String, Result> read(String table, String column,
			List<String> columns) {
		String host = member.getHostName();
        int port = member.getPort();
        //TTransport transport = new TFramedTransport(new TSocket(host, port));
        TTransport transport = getTTransport(host, port);
        try {
        	MDCCAppServerService.Client client = getClient(transport);
            Map<String, ReadValue> reads = client.read2(table, column, columns);
            Map<String, Result> ret = new HashMap<String, Result>();
            for (Entry<String, ReadValue> e : reads.entrySet()) {
            	ReadValue r = e.getValue();
				Result result = new Result(r.getKey(), r.getValue(), r.getVersion(),
						r.classicEndVersion >= r.getVersion());
				ret.put(e.getKey(), result);
            }
            //close(transport);
            return ret;
        } catch (TException e) {
            handleException(host, e);
            return null;
        } finally {
            
        }
	}

	@Override
	public List<Map<String, Result>> read(String table, String key_prefix,
			List<String> columns, String constraintColumn,
			String constraintValue, String orderColumn, boolean isAssending) {
		String host = member.getHostName();
        int port = member.getPort();
        //TTransport transport = new TFramedTransport(new TSocket(host, port));
        TTransport transport = getTTransport(host, port);
        try {
        	MDCCAppServerService.Client client = getClient(transport);
            List<Map<String, ReadValue>> reads = client.read3(table, key_prefix, columns, constraintColumn, constraintValue, orderColumn, isAssending);
            List<Map<String, Result>> ret = new ArrayList<Map<String, Result>>();
            for (Map<String, ReadValue> m : reads) {
            	Map<String, Result> ret_ = new HashMap<String, Result>();
            	for (Entry<String, ReadValue> e : m.entrySet()) {
            		ReadValue r = e.getValue();
            		Result result = new Result(r.getKey(), r.getValue(), r.getVersion(),
    						r.classicEndVersion >= r.getVersion());
            		ret_.put(e.getKey(), result);
            	}
            	ret.add(ret_);
            }
            //close(transport);
            return ret;
        } catch (TException e) {
            handleException(host, e);
            return null;
        } finally {
            
        }
	}

	@Override
	public List<Result> read(String table, String key_prefix,
			String projectionColumn, String constraintColumn, int lowerBound,
			int upperBound) {
		String host = member.getHostName();
        int port = member.getPort();
        //TTransport transport = new TFramedTransport(new TSocket(host, port));
        TTransport transport = getTTransport(host, port);
        try {
        	MDCCAppServerService.Client client = getClient(transport);
			List<ReadValue> reads = client.read4(table, key_prefix,
					projectionColumn, constraintColumn, lowerBound, upperBound);
			List<Result> ret = new ArrayList<Result>();
			for (ReadValue r : reads) {
				Result result = new Result(r.getKey(), r.getValue(), r.getVersion(),
						r.classicEndVersion >= r.getVersion());
				ret.add(result);
			}
			//close(transport);
			return ret;
        } catch (TException e) {
            handleException(host, e);
            return null;
        } finally {
            
        }
	}

	@Override
	public int read(String table, String key_prefix, String constraintColumn,
			int lowerBound, int upperBound) {
		String host = member.getHostName();
        int port = member.getPort();
        //TTransport transport = new TFramedTransport(new TSocket(host, port));
        TTransport transport = getTTransport(host, port);
        try {
        	MDCCAppServerService.Client client = getClient(transport);
        	int ret = client.read5(table, key_prefix, constraintColumn, lowerBound, upperBound);
        	//close(transport);
            return ret;
        } catch (TException e) {
            handleException(host, e);
            return 0;
        } finally {
            
        }
	}

}
