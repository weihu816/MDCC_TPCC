package edu.ucsb.cs.mdcc.messaging;

import edu.ucsb.cs.mdcc.Option;
import edu.ucsb.cs.mdcc.config.Member;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.server.TNonblockingServer;

import edu.ucsb.cs.mdcc.paxos.AgentService;
import edu.ucsb.cs.mdcc.paxos.RecoverySet;
import edu.ucsb.cs.mdcc.paxos.VoteCounter;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MDCCCommunicator {

    private static final Log log = LogFactory.getLog(MDCCCommunicator.class);

    private ExecutorService exec;
    private TServer server;
	
	//start listener to handle incoming calls
    public void startListener(final AgentService agent, final int port) {
        exec = Executors.newSingleThreadExecutor();
        exec.submit(new Runnable() {
            public void run() {
                try {
                    TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(port);
                    MDCCCommunicationService.Processor processor = new MDCCCommunicationService.Processor(
                            new MDCCCommunicationServiceHandler(agent));
                    server = new TNonblockingServer(new TNonblockingServer.Args(serverTransport).
                            processor(processor));
                    log.info("Starting server on port: " + port);
                    server.serve();
                } catch (TTransportException e) {
                    log.error("Error while initializing the Thrift service", e);
                }
            }
        });
    }

    public void stopListener() {
        log.info("Stopping Thrift server");
        server.stop();
        exec.shutdownNow();
    }

    //check whether a node is up and reachable
	public boolean ping(Member member) {
        String host = member.getHostName();
        int port = member.getPort();
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
            MDCCCommunicationService.Client client = getClient(transport);
            return client.ping();
        } catch (TException e) {
            handleException(host, e);
            return false;
        } finally {
            close(transport);
        }
    }
	
	public void sendAcceptAsync(Member member, String transaction,
                                BallotNumber ballot, Option option, VoteCounter voting) {
		try {
            TNonblockingSocket socket = new TNonblockingSocket(member.getHostName(),
                    member.getPort());
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            TAsyncClientManager clientManager = new TAsyncClientManager();
            MDCCCommunicationService.AsyncClient client =
                    new MDCCCommunicationService.AsyncClient(protocolFactory,
                            clientManager, socket);
            client.accept(transaction, option.getKey(),
                    option.getOldVersion(), ballot, option.getValue(), voting);
        } catch (Exception e) {
            voting.onError(e);
            handleException(member.getHostName(), e);
        }
	}
	
	public void sendRecoverAsync(Member member, Map<String,Long> versions, RecoverySet callback) {
		try {
			TNonblockingSocket socket = new TNonblockingSocket(member.getHostName(),
			member.getPort());
			TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
			TAsyncClientManager clientManager = new TAsyncClientManager();
			MDCCCommunicationService.AsyncClient client =
			new MDCCCommunicationService.AsyncClient(protocolFactory,
			        clientManager, socket);
			client.recover(versions, callback);
		} catch (Exception e) {
			callback.onError(e);
		}
	}
	
	public boolean sendDecide(Member member, String transaction, boolean commit) {
        String host = member.getHostName();
        int port = member.getPort();
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
            MDCCCommunicationService.Client client = getClient(transport);
            client.decide(transaction, commit);
            return true;
        } catch (TException e) {
            handleException(host, e);
            return false;
        } finally {
            close(transport);
        }
	}
	
	public ReadValue get(Member member, String key) {
        String host = member.getHostName();
        int port = member.getPort();
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        try {
            MDCCCommunicationService.Client client = getClient(transport);
            return client.read(key);
        } catch (TException e) {
            handleException(host, e);
            return null;
        } finally {
            close(transport);
        }
	}
	
	private MDCCCommunicationService.Client getClient(
            TTransport transport) throws TTransportException {
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new MDCCCommunicationService.Client(protocol);
    }

    private void close(TTransport transport) {
        if (transport.isOpen()) {
            transport.close();
        }
    }

    private void handleException(String target, Exception e) {
        String msg = "Error contacting the remote member: " + target;
        log.debug(msg, e);
    }

}
