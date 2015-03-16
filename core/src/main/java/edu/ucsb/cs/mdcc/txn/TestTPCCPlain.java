package edu.ucsb.cs.mdcc.txn;

import java.util.Random;

import edu.ucsb.cs.mdcc.config.TPCCConfiguration;
import edu.ucsb.cs.mdcc.paxos.TransactionException;

public class TestTPCCPlain{

	/*
	 * Function name: main
	 * Description: the main driver
	 */
	public static Random random = new Random();
	
	/*
	 *  arg1: number of warehouses
	 *  arg2: run time in second
	 */
	public static void main(String[] args) throws TransactionException {

		int num_seconds = TPCCConfiguration.getConfiguration().getTime_seconds();
		// start the tpcc transactions
		TPCC t = new TPCC();
		long start = System.currentTimeMillis();
		int count_neworder = 0;
		long timepassed = 0;
		long timetotal = num_seconds * 1000;
		System.out.println("Running TPCC Transactions for " + num_seconds + " seconds");
		while (timepassed < timetotal) {
			/* Measure how it performs during 1-min interval */
			int x = randomInt(1, 100);
			int y = randomInt(1, 100);
			if (x <= 44) {
				// new order transaction
				t.Neworder(randomInt(1, TPCC.COUNT_WARE), randomInt(1, 10));
			} else if (x <= 87) {
				// payment transaction
				if (y < 60) {
					// 60% by customer last name
					t.Payment(randomInt(1, TPCC.COUNT_WARE), randomInt(1, 10),
							TPCCUtil.Lastname(TPCCUtil.NURand(255, 0, 999)));
				} else {
					// 40% by customer id transaction
					t.Payment(randomInt(1, TPCC.COUNT_WARE), randomInt(1, 10),
							TPCCUtil.NURand(1023, 1, 3000));
				}

			} else if (x <= 91) {
				// Order status transaction
				if (y < 60) {
					// 60% by customer last name
					t.Orderstatus(randomInt(1, 10), TPCCUtil.Lastname(TPCCUtil.NURand(255, 0, 999)));
				} else {
					// 40% by customer id
					t.Orderstatus(randomInt(1, 10), TPCCUtil.NURand(1023, 1, 3000));
				}
			} else if (x <= 95) {
				// delivery transaction
				t.Delivery(randomInt(1, 10));
			} else {
				// stock level transaction
				t.Stocklevel(randomInt(10, 20));
			}
			timepassed = System.currentTimeMillis() - start;
			count_neworder++;
			if (count_neworder % 100 == 0) {
				System.out.print(".");
				if (count_neworder % 2000 == 0) {
					System.out.println();
				}
			}
		}
		System.out.println("\n==============================Result==============================");
		System.out.println(count_neworder + " Transactions finished in " + num_seconds + " seconds");
		System.out.println("==================================================================");
	}

	public static int randomInt(int min, int max) {
		return random.nextInt(max + 1) % (max - min + 1) + min;
	}

}
