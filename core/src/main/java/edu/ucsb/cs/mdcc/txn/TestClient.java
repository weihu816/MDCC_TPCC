package edu.ucsb.cs.mdcc.txn;

import edu.ucsb.cs.mdcc.paxos.TransactionException;


public class TestClient {

    public static void main(String[] args) throws TransactionException {
        TPCC t = new TPCC();
        t.Neworder(1, 1);
        t.Neworder(1, 1);
        t.Neworder(1, 1);
        t.Neworder(1, 1);
    }
}
