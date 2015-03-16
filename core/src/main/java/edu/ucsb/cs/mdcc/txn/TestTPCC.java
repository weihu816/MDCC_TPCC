package edu.ucsb.cs.mdcc.txn;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

import edu.ucsb.cs.mdcc.config.TPCCConfiguration;
import edu.ucsb.cs.mdcc.paxos.TransactionException;

public class TestTPCC {
	
	private static final Log log = LogFactory.getLog(TestTPCC.class);

	public static Random random = new Random();
	
	public static void main(String[] args) throws TransactionException {

		PropertyConfigurator.configure(TPCCConfiguration.getConfiguration().getLogConfigFilePath());
		
		int num_seconds = TPCCConfiguration.getConfiguration().getTime_seconds();
		int wareNum = TPCCConfiguration.getConfiguration().getNum_warehouse();

		// start the tpcc transactions
		int totalNewOrderTxnNum = 0;
		int accounts = wareNum * TPCCConfiguration.getConfiguration().TermMultiplication;
		ExecutorService exec = Executors.newFixedThreadPool(accounts);
        Future<Integer>[] futures = new Future[accounts];
        for (int i = 0; i < accounts; i++) {
            futures[i] = exec.submit(new TPCCTerminal(num_seconds, i));
        }

        for (int i = 0; i < accounts; i++) {
            try {
            	totalNewOrderTxnNum += futures[i].get(num_seconds+30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            	log.info("=======[[["+i+"]]]======");
            	e.printStackTrace();
            } catch (ExecutionException e) {
            	log.info("=======[[["+i+"]]]======");
            	e.printStackTrace();
            } catch (TimeoutException e) {
            	log.info("=======[[["+i+"]]]======");
				e.printStackTrace();
			}
        }
        exec.shutdownNow();

		log.info("==============================Result==============================");
		log.info(totalNewOrderTxnNum + " ***Plain*** Transactions finished in " + 
							num_seconds + " seconds with "+ 
							accounts + " terminals");
		log.info("==================================================================");
	}

	public static AtomicInteger totalRetries = new AtomicInteger(0);
	public static String resultFile = TPCCConfiguration.getConfiguration().getOutputFile();
	private static synchronized void outputArrayToFile(String fname, String type, ArrayList<Long> start, ArrayList<Long> end, String msg){
		File dir = new File(TPCCConfiguration.getConfiguration().getResultDir());
		if(!dir.exists())
			dir.mkdirs();
		
		try {
			OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(dir+"/"+fname, true));
			int pos = 0;
			for(Long stime : start){
				//writer.write(type+"\t"+stime+"\t"+end.get(pos)+"\t"+(end.get(pos)-stime)+"\t"+msg+"\n");
				pos++;
			}
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static int randomInt(int min, int max) {
		return random.nextInt(max + 1) % (max - min + 1) + min;
	}
	
	private static class TPCCTerminal implements Callable<Integer>{

		private TPCC t;
		private int num_seconds;
		private final int termID;
		
		private ArrayList<Long> noStart, noEnd, pStart, pEnd, osStart, osEnd, dStart, dEnd, slStart, slEnd;
		
		private TPCCTerminal(int durationSeconds, int id){
			termID = id;
			t = new TPCC();
			this.num_seconds = durationSeconds;
			
			noStart = new ArrayList<Long>();
			noEnd = new ArrayList<Long>();
			pStart = new ArrayList<Long>();
			pEnd = new ArrayList<Long>();
			osStart = new ArrayList<Long>();
			osEnd = new ArrayList<Long>();
			dStart = new ArrayList<Long>();
			dEnd = new ArrayList<Long>();
			slStart = new ArrayList<Long>();
			slEnd = new ArrayList<Long>();
		}

		private void outputMetricsToFile(){
			outputArrayToFile(resultFile, "NewOrder", noStart, noEnd, String.valueOf(termID));
			outputArrayToFile(resultFile, "Payment", pStart, pEnd, String.valueOf(termID));
			outputArrayToFile(resultFile, "OrderStatus", osStart, osEnd, String.valueOf(termID));
			outputArrayToFile(resultFile, "Delivery", dStart, dEnd, String.valueOf(termID));
			outputArrayToFile(resultFile, "StockLevel", slStart, slEnd, String.valueOf(termID));
		}
		
		@Override
		public Integer call() throws Exception {
			
			int wareNum = TPCCConfiguration.getConfiguration().getNum_warehouse();
			
			// start the tpcc transactions
			long start = System.currentTimeMillis();
			int count_neworder = 0;
			long timepassed = 0;
			long timetotal = num_seconds * 1000;
			log.info("[[[" + termID + "]]] Running ***Plain*** TPCC Transactions for " + num_seconds + " seconds");
			while (timepassed < timetotal) {
				/* Measure how it performs during 1-min interval */
				int x = randomInt(1, 100);
				int y = randomInt(1, 100);
				try {
					if (x <= 44) {
						// new order transaction
						noStart.add(System.currentTimeMillis());
						t.Neworder(randomInt(1, wareNum), randomInt(1, 10));
						noEnd.add(System.currentTimeMillis());
					} else if (x <= 87) {
						// payment transaction
						if (y < 60) {
							// 60% by customer last name
							pStart.add(System.currentTimeMillis());
							t.Payment(randomInt(1, wareNum), randomInt(1, 10),
									TPCCUtil.Lastname(TPCCUtil.NURand(255, 0, 999)));
							pEnd.add(System.currentTimeMillis());
						} else {
							pStart.add(System.currentTimeMillis());
							// 40% by customer id transaction
							t.Payment(randomInt(1, wareNum), randomInt(1, 10),
									TPCCUtil.NURand(1023, 1, 3000));
							pEnd.add(System.currentTimeMillis());
						}
	
					} else if (x <= 91) {
						// Order status transaction
						if (y < 60) {
							// 60% by customer last name
							osStart.add(System.currentTimeMillis());
							t.Orderstatus(randomInt(1, 10), TPCCUtil.Lastname(TPCCUtil.NURand(255, 0, 999)));
							osEnd.add(System.currentTimeMillis());
						} else {
							// 40% by customer id
							osStart.add(System.currentTimeMillis());
							t.Orderstatus(randomInt(1, 10), TPCCUtil.NURand(1023, 1, 3000));
							osEnd.add(System.currentTimeMillis());
						}
					} else if (x <= 95) {
						// delivery transaction
						dStart.add(System.currentTimeMillis());
						t.Delivery(randomInt(1, 10));
						dEnd.add(System.currentTimeMillis());
					} else {
						// stock level transaction
						slStart.add(System.currentTimeMillis());
						t.Stocklevel(randomInt(10, 20));
						slEnd.add(System.currentTimeMillis());
					}
				}  catch (TransactionException e) {
					continue;
				}
				timepassed = System.currentTimeMillis() - start;
				count_neworder++;
				if (count_neworder % 100 == 0) {
					System.out.print(".");
					if (count_neworder % 2000 == 0) {
						System.out.println("");
					}
				}
			}
			
			log.info("[[[" + termID + "]]] runs {{{" + count_neworder + "}}} TPCC Transactions for " + timepassed/1000 + " seconds");
			log.info("[[[" + termID + "]]] times of retry {{{" + t.retryNums + "}}}");
			outputMetricsToFile();
			return count_neworder;
		}
	}

}
