package edu.ucsb.cs.mdcc.txn;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsb.cs.mdcc.Result;
import edu.ucsb.cs.mdcc.config.TPCCConfiguration;
import edu.ucsb.cs.mdcc.paxos.TransactionException;

public class TPCC {

	final TransactionFactory fac = new TransactionFactory();
	
	public ArrayList<Long> noStart, noEnd, pStart, pEnd, osStart, osEnd, dStart, dEnd, slStart, slEnd;
	
//	public final int backoffT, backoffB;
//	long durationInMilli,startTime=-1;
	
	public static int COUNT_WARE = TPCCConfiguration.getConfiguration().getNum_warehouse();
	boolean flag_output = true;

	public TPCC() {

		this.flag_output = TPCCConfiguration.getConfiguration().getFlagOutput();

//		this.backoffB = TPCCConfiguration.getConfiguration().getBackoffBase();
//		this.backoffT = TPCCConfiguration.getConfiguration().getBackoffTimes();
//		this.durationInMilli = TPCCConfiguration.getConfiguration().getTime_seconds()*1000;	
//		noStart = new ArrayList<Long>();
//		noEnd = new ArrayList<Long>();
//		pStart = new ArrayList<Long>();
//		pEnd = new ArrayList<Long>();
//		osStart = new ArrayList<Long>();
//		osEnd = new ArrayList<Long>();
//		dStart = new ArrayList<Long>();
//		dEnd = new ArrayList<Long>();
//		slStart = new ArrayList<Long>();
//		slEnd = new ArrayList<Long>();
	}
	//===============================================================
	//===============================================================

//	public void setStartTime(){
//		this.startTime = System.currentTimeMillis();
//	}
	
//	private boolean isOutOfTime(){
//		return this.durationInMilli<System.currentTimeMillis()-startTime;
//	}
	public static int getCOUNT_WARE() {
		return COUNT_WARE;
	}

	public static void setCOUNT_WARE(int c) {
		COUNT_WARE = c;
	}
	
	//===============================================================
	//===============================================================

	
	public void Neworder(int w_id, int d_id) throws TransactionException {
		
		MDCCTransaction t = fac.create();
		t.begin();

		/* local variables */
		int d_next_o_id = 0, o_id = 0, o_all_local = 1, c_id = TPCCUtil.NURand(TPCCConstant.A_C_ID, 1, TPCCConstant.CUST_PER_DIST), o_ol_cnt = TPCCUtil.randomInt(5, 15);
		float w_tax = 0, d_tax = 0, c_discount = 0;
		String o_entry_d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		Map<String, Result> result;
		boolean valid = true;
		List<String> columns;
		List<String> values;

		/* retrieve TPCCConstant.WAREHOUSE information  */
		String key_WAREHOUSE = String.valueOf(w_id);
		columns = TPCCUtil.buildColumns("w_tax");
		result = t.doRead(TPCCConstant.WAREHOUSE, key_WAREHOUSE, columns);
		w_tax = Float.valueOf(new String(result.get("w_tax").getValue()));


		/* retrieve customer information */
		String c_last = null, c_credit = null;
		String key_customer = (TPCCUtil.buildString(w_id, "_", d_id, "_", c_id));
		columns = TPCCUtil.buildColumns ("c_last", "c_discount", "c_credit");
		result = t.doRead(TPCCConstant.CUSTOMER, key_customer, columns);
		c_last = new String(result.get("c_last").getValue());
		c_discount = Float.valueOf(new String(result.get("c_discount").getValue()));
		c_credit = new String(result.get("c_credit").getValue());


		/* retrieve district information  */
		String key_district = (TPCCUtil.buildString(w_id, "_", d_id));
		columns = TPCCUtil.buildColumns("d_next_o_id", "d_tax");
		result = t.doRead(TPCCConstant.DISTRICT, key_district, columns);
		d_next_o_id = Integer.valueOf(new String(result.get("d_next_o_id").getValue()));
		d_tax = Float.valueOf(new String(result.get("d_tax").getValue()));


		/* increase d_next_o_id by one */
		columns = TPCCUtil.buildColumns("d_next_o_id");
		values = TPCCUtil.buildColumns(d_next_o_id+1);
		t.write(TPCCConstant.DISTRICT, key_district, columns, values, 1);

		/* build supware for each order line */
		int supware			[] 	= new int	[o_ol_cnt];
		int ol_i_ids		[] 	= new int	[o_ol_cnt];
		int ol_quantities	[] 	= new int	[o_ol_cnt];
		int s_quantities	[] 	= new int	[o_ol_cnt];
		String i_names		[] 	= new String[o_ol_cnt];
		float i_prices		[] 	= new float	[o_ol_cnt];
		float ol_amounts	[] 	= new float	[o_ol_cnt];
		char bg				[] 	= new char	[o_ol_cnt];

		for (int ol_number = 1; ol_number<=o_ol_cnt; ol_number++) {

			int ol_supply_w_id; 
			/* 99% of supply are from home stock*/
			if (TPCCUtil.randomInt(0, 99) == 0 && COUNT_WARE > 1) {
				int supply_w_id = TPCCUtil.randomInt(1, COUNT_WARE); 
				while (supply_w_id == w_id) {
					supply_w_id = TPCCUtil.randomInt(1, COUNT_WARE); 
				}
				ol_supply_w_id = supply_w_id;
			} else {
				ol_supply_w_id = w_id;
			}
			if (ol_supply_w_id != w_id) {
				o_all_local = 0; 
			}
			supware[ol_number - 1] = ol_supply_w_id;
			ol_i_ids[ol_number - 1] = TPCCUtil.NURand(TPCCConstant.A_OL_I_ID,1,100000);
			/* rbk is used for 1% of error */
			int rbk = TPCCUtil.randomInt(1, 100);
			if(rbk == 1) {
				ol_i_ids[ol_number - 1] = TPCCUtil.randomInt(200001, 300000);
			}
		}

		/* assign d_next_o_id to o_id */
		o_id = d_next_o_id;
		String key_order = (TPCCUtil.buildString(w_id, "_", d_id, "_", o_id));
		/* insert into new order table and order table*/
		columns = TPCCUtil.buildColumns("o_id", "o_d_id", "o_w_id",
				"o_c_id", "o_entry_d", "o_carrier_id",
				"o_ol_cnt", "o_all_local");
		values = TPCCUtil.buildColumns((o_id), (d_id), (w_id), (c_id),
				(o_entry_d), "NULL", (o_ol_cnt), (o_all_local));
		t.write(TPCCConstant.ORDER, key_order, columns, values, 0);

		columns = TPCCUtil.buildColumns("no_o_id", "no_d_id", "no_w_id");
		values = TPCCUtil.buildColumns(o_id, d_id, w_id);
		t.write(TPCCConstant.NEWORDER, key_order, columns, values, 0);


		/* for each order in the order line*/
		for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {

			int ol_supply_w_id = supware[ol_number - 1]; 
			int ol_i_id = ol_i_ids[ol_number - 1]; 
			int ol_quantity = TPCCUtil.randomInt(1, 10);
			int s_quantity = 0;
			float i_price = 0.0f, ol_amount = 0.0f;
			String i_name = null, ol_dist_info = null, i_data = null;

			/* retrieve item information */
			String key_item = String.valueOf(ol_i_id);
			columns = TPCCUtil.buildColumns("i_price", "i_name", "i_data");
			result = t.doRead(TPCCConstant.ITEM, key_item, columns);			

			if (result == null || result.size() == 0) {
				/* roll back new order and return */
				columns = TPCCUtil.buildColumns("o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_ol_cnt", "o_all_local", "o_carrier_id");
				values = null;
				t.write(TPCCConstant.ORDER, key_order, columns, values, 2);
				columns = TPCCUtil.buildColumns("no_o_id", "no_w_id", "no_d_id");
				t.write(TPCCConstant.NEWORDER, key_order, columns, values, 2);
				for (int i = 1; i < ol_number; i++) {
					String key_ORDERLINE = (TPCCUtil.buildString( w_id, "_", d_id, "_", o_id, "_", i));
					columns = TPCCUtil.buildColumns("ol_o_id", "ol_d_id", "ol_w_id",
							"ol_number", "ol_i_id", "ol_supply_w_id",
							"ol_quantity", "ol_dist_info", "ol_amount",
							"ol_delivery_d");
					t.write(TPCCConstant.ORDERLINE, key_ORDERLINE, columns, values, 2);
				}
				columns = TPCCUtil.buildColumns("d_next_o_id");
				values = TPCCUtil.buildColumns(d_next_o_id);
				t.write(TPCCConstant.DISTRICT, key_district, columns, values, 1);
				valid = false;
				break;
			}

			i_price = Float.valueOf(new String(result.get("i_price").getValue()));
			i_name = new String(result.get("i_name").getValue());
			i_data = new String(result.get("i_data").getValue());

			/* retrieve stock information */
			String s_dist[] = new String[TPCCConstant.DIST_PER_WARE];
			String s_data = null;
			String key_stock = TPCCUtil.buildString(ol_supply_w_id, "_", ol_i_id);
			columns = TPCCUtil.buildColumns("s_quantity", "s_dist_01", "s_dist_02",
					"s_dist_03", "s_dist_04", "s_dist_05", "s_dist_06",
					"s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10",
					"s_data");
			result = t.doRead(TPCCConstant.STOCK, key_stock, columns);

			s_quantity = Integer.valueOf(new String(result.get("s_quantity").getValue()));
			for (int i = 1; i < 10; i++) {
				s_dist[i-1] = new String(result.get("s_dist_0" + i).getValue());
			}
			s_dist[9] = new String(result.get("s_dist_10").getValue());
			s_data = new String(result.get("s_data").getValue());


			ol_dist_info = s_dist[d_id-1];


			if ( i_data != null && s_data != null && (i_data.indexOf("original") != -1) && (s_data.indexOf("original") != -1) ) {
				bg[ol_number-1] = 'B'; 
			} else {
				bg[ol_number-1] = 'G';
			}

			if (s_quantity > ol_quantity) {
				s_quantity = s_quantity - ol_quantity;
			} else {
				s_quantity = s_quantity - ol_quantity + 91;
			}

			/* update stock quantity */
			columns = TPCCUtil.buildColumns("s_quantity");
			values = TPCCUtil.buildColumns(s_quantity);
			t.write(TPCCConstant.STOCK, key_stock, columns, values, 1);

			/* calculate order-line amount*/
			ol_amount = ol_quantity * i_price *(1+w_tax+d_tax) *(1-c_discount); 

			/* insert into order line table */
			String key_ORDERLINE = TPCCUtil.buildString(w_id, "_", d_id , "_" , o_id , "_", ol_number);
			columns = TPCCUtil.buildColumns("ol_o_id", "ol_d_id", "ol_w_id",
					"ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d",
					"ol_quantity", "ol_amount", "ol_dist_info");
			values = TPCCUtil.buildColumns(o_id, d_id, w_id, ol_number, ol_i_id,
					ol_supply_w_id, "NULL", ol_quantity, ol_amount,
					ol_dist_info);
			t.write(TPCCConstant.ORDERLINE, key_ORDERLINE, columns, values, 0);

			i_names			[ol_number - 1] = i_name;
			i_prices		[ol_number - 1] = i_price;
			ol_amounts		[ol_number - 1] = ol_amount;
			ol_quantities	[ol_number - 1] = ol_quantity;
			s_quantities	[ol_number - 1] = s_quantity;
		}


		t.commit();		
		
		if (!flag_output) {
			return;
		}
		System.out.println("==============================New Order==================================");
		System.out.println("TPCCConstant.WAREHOUSE: " + w_id + "\tDistrict: " + d_id);
		if (valid) {
			System.out.println("Customer: " + c_id + "\tName: " + c_last + "\tCredit: " + c_credit + "\tDiscount: " + c_discount);
			System.out.println("Order Number: " + o_id + " OrderId: " + o_id + " Number_Lines: " + o_ol_cnt + " W_tax: " + w_tax + " D_tax: " + d_tax + "\n");
			System.out.println("Supp_W Item_Id           Item Name     ol_q s_q  bg Price Amount");
			for (int i = 0; i < o_ol_cnt; i++) {
				System.out.println( String.format("  %4d %6d %24s %2d %4d %3c %6.2f %6.2f",
						supware[i], ol_i_ids[i], i_names[i], ol_quantities[i], s_quantities[i], bg[i], i_prices[i], ol_amounts[i]));
			}
		} else {
			System.out.println("Customer: " + c_id + "\tName: " + c_last + "\tCredit: " + c_credit + "\tOrderId: " + o_id);
			System.out.println("Exection Status: Item number is not valid");
		}
		System.out.println("=========================================================================");

		

	}
	
	/*
	 * Function name: Payment
	 * Description: The Payment business transaction updates the customer's balance and reflects the payment
	 * 				on the district and TPCCConstant.WAREHOUSE sales statistics.
	 * Argument: 	d_id － randomly selected within [1 .. 10]
	 * 				c_id_or_c_last － 60% c_last random TPCCGenerator.NURand(255,0,999) | 40% c_id - random TPCCGenerator.NURand(1023,1,3000)
	 */
	public void Payment(int w_id, int d_id, Object c_id_or_c_last) throws TransactionException {

		
		MDCCTransaction t = fac.create();
		t.begin();

		/* c_id or c_last */
		Boolean byname = false;
		if (c_id_or_c_last instanceof String) {
			byname = true;
		}

		/* local variables */
		float h_amount = TPCCUtil.randomFloat(0, 5000);
		int x = TPCCUtil.randomInt(1, 100);
		/*  the customer resident TPCCConstant.WAREHOUSE is the home 85% , remote 15% of the time  */
		int c_d_id, c_w_id;
		if (x <= 85 ) { 
			c_w_id = w_id;
			c_d_id = d_id;
		} else {
			c_d_id = TPCCUtil.randomInt(1, 10);
			do {
				c_w_id = TPCCUtil.randomInt(1, COUNT_WARE);
			} while (c_w_id == w_id && COUNT_WARE > 1);
		}
		String h_date = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		Map<String, Result> result;
		List<Map<String, Result>> results;
		List<String> columns;
		List<String> values;


		/* retrieve and update TPCCConstant.WAREHOUSE w_ytd */
		float w_ytd = 0;
		String w_name = null, w_street_1 = null, w_street_2 = null, w_city = null, w_state = null, w_zip = null;
		String key_WAREHOUSE = String.valueOf(w_id);
		columns = TPCCUtil.buildColumns("w_ytd", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip");
		result = t.doRead(TPCCConstant.WAREHOUSE, key_WAREHOUSE, columns);

		w_ytd = Float.valueOf(new String(result.get("w_ytd").getValue()));
		w_name = new String(result.get("w_name").getValue());
		w_street_1 = new String(result.get("w_street_1").getValue());
		w_street_2 = new String (result.get("w_street_2").getValue());
		w_city = new String(result.get("w_city").getValue());
		w_state = new String(result.get("w_state").getValue());
		w_zip = new String(result.get("w_zip").getValue());

		w_ytd += h_amount;

		columns = TPCCUtil.buildColumns("w_ytd");
		values = TPCCUtil.buildColumns(w_ytd);
		t.write(TPCCConstant.WAREHOUSE, key_WAREHOUSE, columns, values, 1);

		/* retrieve and update district d_ytd */
		float d_ytd = 0;
		String d_name = null,  d_street_1 = null, d_street_2 = null, d_city = null, d_state = null, d_zip = null;
		String key_district = TPCCUtil.buildString(w_id, "_", d_id);
		columns = TPCCUtil.buildColumns("d_ytd", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip");
		result = t.doRead(TPCCConstant.DISTRICT, key_district, columns);
		d_ytd = Float.valueOf(new String(result.get("d_ytd").getValue()));
		d_name = new String(result.get("d_name").getValue());
		d_street_1 = new String(result.get("d_street_1").getValue());
		d_street_2 = new String(result.get("d_street_2").getValue());
		d_city = new String(result.get("d_city").getValue());
		d_state = new String(result.get("d_state").getValue());
		d_zip = new String(result.get("d_zip").getValue());

		/* update district d_ytd */
		d_ytd += h_amount;
		columns = TPCCUtil.buildColumns("d_ytd");
		values = TPCCUtil.buildColumns(d_ytd);
		t.write(TPCCConstant.DISTRICT, key_district, columns, values, 1);

		/* retrieve customer information */
		float c_balance = 0.0f;
		String c_data = null, h_data = null, c_first = null, c_middle = null, c_last = null;
		String c_street_1 = null, c_street_2 = null, c_city = null, c_state = null, c_zip = null;
		String c_phone = null, c_credit = null, c_credit_lim = null, c_since = null;
		int c_id = 0;
		String key_customer = null, key_prefix_customer = null;
		if (byname) {

			key_prefix_customer = (TPCCUtil.buildString(c_w_id + "_" + c_d_id));
			columns = TPCCUtil.buildColumns("c_id", "c_balance", "c_data",
					"c_first", "c_middle", "c_last", "c_street_1",
					"c_street_2", "c_city", "c_state", "c_zip", "c_phone",
					"c_credit", "c_credit_lim", "c_since");
			String constraintColumn = "c_last";
			String ConstraintValue = ((String)c_id_or_c_last);
			String orderColumn =  ("c_first");
			results = t.doRead(TPCCConstant.CUSTOMER, key_prefix_customer, columns, constraintColumn, ConstraintValue, orderColumn, false);
			int index = results.size() / 2;
			result = results.get(index);
			/* ORDER BY c_first and get midpoint */
			c_id = Integer.valueOf(new String(result.get("c_id").getValue()));
			c_balance = Float.valueOf(new String(result.get("c_balance").getValue()));
			c_data = new String(result.get("c_data").getValue());
			c_first = new String(result.get("c_first").getValue());
			c_middle = new String(result.get("c_middle").getValue());
			c_last = new String(result.get("c_last").getValue());
			c_street_1 = new String(result.get("c_street_1").getValue());
			c_street_2 = new String(result.get("c_street_2").getValue());
			c_city = new String(result.get("c_city").getValue());
			c_state = new String(result.get("c_state").getValue());
			c_zip = new String(result.get("c_zip").getValue());
			c_phone = new String(result.get("c_phone").getValue());
			c_credit = new String(result.get("c_credit").getValue());
			c_credit_lim = new String(result.get("c_credit_lim").getValue());
			c_since = new String(result.get("c_since").getValue());
			key_customer = TPCCUtil.buildString(c_w_id, "_", c_d_id, "_", c_id);
		} else {
			key_customer = TPCCUtil.buildString(c_w_id, "_", c_d_id, "_", c_id_or_c_last);
			columns = TPCCUtil.buildColumns("c_balance", "c_credit", "c_data",
					"c_first", "c_middle", "c_last", "c_street_1",
					"c_street_2", "c_city", "c_state", "c_zip", "c_phone",
					"c_credit", "c_credit_lim", "c_since");
			result = t.doRead(TPCCConstant.CUSTOMER, key_customer, columns);
			c_id = (int) c_id_or_c_last;
			c_balance = Float.valueOf(new String(result.get("c_balance").getValue()));
			c_data = new String(result.get("c_data").getValue());
			c_first = new String(result.get("c_first").getValue());
			c_middle = new String(result.get("c_middle").getValue());
			c_last = new String(result.get("c_last").getValue());
			c_street_1 = new String(result.get("c_street_1").getValue());
			c_street_2 = new String(result.get("c_street_2").getValue());
			c_city = new String(result.get("c_city").getValue());
			c_state = new String(result.get("c_state").getValue());
			c_zip = new String(result.get("c_zip").getValue());
			c_phone = new String(result.get("c_phone").getValue());
			c_credit = new String(result.get("c_credit").getValue());
			c_credit_lim = new String(result.get("c_credit_lim").getValue());
			c_since = new String(result.get("c_since").getValue());
			key_customer = TPCCUtil.buildString(c_w_id, "_", c_d_id, "_", c_id);
		}


		c_balance -= h_amount;
		h_data = w_name + "    " + d_name;
		if (c_credit.equals("BC")) {
			String c_new_data = String.format("| %4d %2d %4d %2d %4d $%7.2f %12s %24s", 
					c_id,c_d_id, c_w_id, d_id, w_id, h_amount, h_date, h_data);
			c_new_data += c_data;

			/* update customer c_balance， c_data */
			columns = TPCCUtil.buildColumns("c_balance", "c_data");
			values = TPCCUtil.buildColumns(c_balance, c_new_data);
			t.write(TPCCConstant.CUSTOMER, key_customer, columns, values, 1);

		} else {
			/* update customer c_balance */
			columns = TPCCUtil.buildColumns("c_balance");
			values = TPCCUtil.buildColumns(c_balance);
			t.write(TPCCConstant.CUSTOMER, key_customer, columns, values, 1);
		}


		/* retrieve history key */
		String key_history = String.valueOf(System.currentTimeMillis());
		/* insert into history table */
		columns = TPCCUtil.buildColumns("h_c_d_id", "h_c_w_id", "h_c_id", "h_d_id",
				"h_w_id", "h_date", "h_amount", "h_data");
		values = TPCCUtil.buildColumns(c_d_id, c_w_id, c_id, d_id, w_id, h_date,
				h_amount, h_data);
		t.write(TPCCConstant.HISTORY, key_history, columns, values, 0);

		t.commit();

		//lock_payment.set(false);
		
		if (!flag_output) {
			return;
		}
		System.out.println("==============================Payment====================================");
		System.out.println("Date: " + h_date + " District: " + d_id);
		System.out.println("TPCCConstant.WAREHOUSE: " + w_id + "\t\t\tDistrict");
		System.out.println(w_street_1 + "\t\t\t\t" + d_street_1);
		System.out.println(w_street_2 + "\t\t\t" + d_street_2);
		System.out.println(w_city + " " + w_state + " " + w_zip + "\t" + d_city + " " + d_state + " " + d_zip);
		System.out.println("");
		System.out.println("Customer: " + c_id + "\tCustomer-TPCCConstant.WAREHOUSE: " + c_w_id + "\tCustomer-District: " + c_d_id);
		System.out.println("Name:" + c_first + " " + c_middle + " " + c_last + "\tCust-Since:" + c_since);
		System.out.println(c_street_1 + "\t\t\tCust-Credit:" + c_credit);
		System.out.println(c_street_2);
		System.out.println(c_city + " " + c_state + " " + c_zip + " \tCust-Phone:" + c_phone);
		System.out.println("");
		System.out.println("Amount Paid:" + h_amount  + "\t\t\tNew Cust-Balance: " + c_balance);
		System.out.println("Credit Limit:" + c_credit_lim);
		System.out.println("");
		if (c_credit.equals("BC")) {
			c_data = c_data.substring(0, 200);
		} 
		int length = c_data.length();
		int n = 50;
		int num_line = length / n;
		if (length % n != 0) num_line += 1;
		System.out.println( "Cust-data: \t" + c_data.substring(0, n));
		for (int i = 1; i < num_line - 1; i++) {
			System.out.println("\t\t" + c_data.substring(n*i, n*(i+1)));
		}
		System.out.println("\t\t" + c_data.substring(n*(num_line-1)));
		System.out.println("=========================================================================");
			
		
		
	}
	
	
	/*
	 * Function name: Orderstatus
	 * Description: The Order-Status business transaction queries the status of a customer's last order. 
	 * Argument: 	d_id － randomly selected within [1 .. 10]
	 * 				c_id_or_c_last - 60% c_last random TPCCGenerator.NURand(255,0,999) | 40% c_id random TPCCGenerator.NURand(1023,1,3000) 
	 */
	public void Orderstatus(int d_id, Object c_id_or_c_last) throws TransactionException {
		
		MDCCTransaction t = fac.create();
		t.begin();
		
		/* local variables */
		Boolean byname = false;
		int w_id = TPCCUtil.randomInt(1, COUNT_WARE);
		float c_balance = 0.0f;
		String c_first = null, c_middle = null, c_last = null;
		
		if (c_id_or_c_last instanceof String) {
			byname = true;
		} 
		
		Map<String, Result> result;
		List<Map<String, Result>> results;
		List<String> columns;
		
		
		/* retrieve customer information */
		int c_id = 0;
		if (byname) {
			/* ORDER BY c_first and Choose the middle point */
			String key_prefix_customer = TPCCUtil.buildString(w_id, "_", d_id);
			String constraintColumn = "c_last";
			String ConstraintValue = (String)c_id_or_c_last;
			String orderColumn = "c_first";
			Boolean isAssending = true;
			columns = TPCCUtil.buildColumns("c_id", "c_balance", "c_first", "c_middle", "c_last");
			results = t.doRead(TPCCConstant.CUSTOMER, key_prefix_customer, columns, constraintColumn, ConstraintValue, orderColumn, isAssending);

			int size = results.size();
			c_id = Integer.valueOf(new String(results.get(size / 2).get("c_id").getValue()));
			c_balance = Float.valueOf(new String(results.get(size / 2).get("c_balance").getValue()));
			c_first = new String(results.get(size / 2).get("c_first").getValue());
			c_middle = new String(results.get(size / 2).get("c_middle").getValue());
			c_last = new String(results.get(size / 2).get("c_last").getValue());

		} else {
			c_id = (int) c_id_or_c_last;
			String key_customer = TPCCUtil.buildString(w_id, "_", d_id, "_", c_id);
			columns = TPCCUtil.buildColumns("c_balance", "c_first","c_middle", "c_last");
			result = t.doRead(TPCCConstant.CUSTOMER, key_customer, columns);
			c_balance = Float.valueOf(new String(result.get("c_balance").getValue()));
			c_first = new String(result.get("c_first").getValue());
			c_middle = new String(result.get("c_middle").getValue());
			c_last = new String(result.get("c_last").getValue());
		}
		
		/* retrieve an order */
		int o_id = 0;
		String o_carrier_id = null;
		String o_entry_d = null;
		
		/* ORDER BY o_id DESC and get the greatest one */
		String key_prefix_order = (TPCCUtil.buildString(w_id, "_", d_id));
		columns = TPCCUtil.buildColumns("o_id", "o_carrier_id", "o_entry_d");
		String constraintColumn = ("o_c_id");
		String constraintValue = String.valueOf(c_id);
		String orderColumn = "o_id";
		results = t.doRead(TPCCConstant.ORDER, key_prefix_order, columns, constraintColumn, constraintValue, orderColumn, false);
		
		if (results.size() == 0) {
			return;
		}
		o_id = Integer.valueOf(new String(results.get(0).get("o_id").getValue()));
		o_carrier_id = new String(results.get(0).get("o_carrier_id").getValue());
		o_entry_d = new String(results.get(0).get("o_entry_d").getValue());
		
		/* retrieve order_line information */
		int ol_i_id = 0, ol_supply_w_id = 0, ol_quantity = 0;
		float ol_amount = 0.0f;
		
		String key_prefix_ORDERLINE = (TPCCUtil.buildString(w_id, "_", d_id, "_", o_id));
		columns = TPCCUtil.buildColumns("ol_i_id", "ol_supply_w_id", "ol_quantity", "ol_amount");
		constraintColumn = null;
		constraintValue = null;
		orderColumn = null;
		results = t.doRead(TPCCConstant.ORDERLINE, key_prefix_ORDERLINE, columns, constraintColumn, constraintValue, orderColumn, false);


		t.commit();

		if (!flag_output) {
			return;
		}
		System.out.println("==============================Order Status===============================");
		System.out.println("TPCCConstant.WAREHOUSE: " + w_id + " District: " + d_id);
		System.out.println("Customer: " + c_id + " Name: " + c_first + " " + c_middle +  " " + c_last);
		System.out.println("Cust-Balance: $ " + c_balance);
		System.out.println("");
		System.out.println("Order-Number: " + o_id + " Entry-Date: " + o_entry_d + " Carrier-Id: " + o_carrier_id);
		System.out.println("Supply-W\tItem-ID\t\tQty\tAmount");
		for (Map<String, Result> list : results) {
			ol_i_id = Integer.valueOf(new String(list.get("ol_i_id").getValue()));
			ol_supply_w_id = Integer.valueOf(new String(list.get("ol_supply_w_id").getValue()));
			ol_quantity = Integer.valueOf(new String(list.get("ol_quantity").getValue()));
			ol_amount = Float.valueOf(new String(list.get("ol_amount").getValue()));
			System.out.println(ol_supply_w_id + "\t\t" + ol_i_id + "\t\t" + ol_quantity + "\t" + ol_amount);
		}
		System.out.println("=========================================================================");
		

	}
	
	public int retryNums=0;
//	private void randomBackoff(){
//		try {
//			long sleepingTime = (long) (random.nextInt(100)* this.backoffT+this.backoffB);
//			//log.info("ZYQ Sleeping for "+sleepingTime);
//			retryNums++;
//			Thread.sleep(sleepingTime);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}//random backoff
//	}
	
	
	/*
	 * Function name: Delivery
	 * Description: The Delivery business transaction consists of processing a batch of 10 new (not yet delivered) orders.
	 * 				Each order is processed (delivered) in full within the scope of a read-write database transaction.
	 * Argument: o_carrier_id - randomly selected within [1 .. 10]
	 */
	public void Delivery(int o_carrier_id) throws TransactionException {
		

		MDCCTransaction t = fac.create();
		t.begin();

		/* local variables */
		int w_id = TPCCUtil.randomInt(1, COUNT_WARE), d_id = TPCCUtil.randomInt(1, TPCCConstant.DIST_PER_WARE);
		Map<String, Result> result;
		List<Map<String, Result>> results;
		List<String> columns;
		List<String> values;

		/* choose an new order */
		int no_o_id = 0;
		/* ORDER BY no_o_id ASC*/
		String key_prefix_neworder = (TPCCUtil.buildString(w_id, "_", d_id));
		columns = TPCCUtil.buildColumns("no_o_id");
		String orderColumn = ("no_o_id");
		results = t.doRead(TPCCConstant.NEWORDER, key_prefix_neworder, columns, null, null, orderColumn, true);

		if (results.size() == 0) {
			/*  If no matching row is found, then the delivery of an order for this district is skipped. */
			return;
		}
		// TODO [wei]
		// __MDCC_DELETE__
		try {
			no_o_id = Integer.parseInt(new String(results.get(0).get("no_o_id").getValue()));
		} catch (NumberFormatException e) {
			// TODO: handle exception
			return;
		}

		/* delete this new order for delivery */
		String key_neworder = TPCCUtil.buildString(w_id, "_", d_id, "_", no_o_id);
		columns = TPCCUtil.buildColumns("no_o_id", "no_w_id", "no_d_id");
		t.write(TPCCConstant.NEWORDER, key_neworder, columns, null, 2);

		/* get the customer id for this order */
		String key_order = TPCCUtil.buildString(w_id, "_", d_id, "_", no_o_id);
		columns = TPCCUtil.buildColumns("o_c_id");
		result = t.doRead(TPCCConstant.ORDER, key_order, columns);
		int o_c_id = Integer.valueOf(new String(result.get("o_c_id").getValue()));

		/* set up the customer key */
		String key_customer = TPCCUtil.buildString(w_id, "_", d_id, "_", o_c_id);

		/* set carrier id for order */
		columns = TPCCUtil.buildColumns("o_carrier_id");
		values = TPCCUtil.buildColumns(o_carrier_id);
		t.write(TPCCConstant.ORDER, key_order, columns, values, 1);

		/* set deliver time for order line */
		/* calculate the total amount for all items */
		String ol_delivery_d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		float ol_total = 0.0f;
		String ol_number = null;
		String key_prefix_ORDERLINE = (TPCCUtil.buildString(w_id, "_", d_id, "_", no_o_id));
		columns = TPCCUtil.buildColumns("ol_number", "ol_amount");
		results = t.doRead(TPCCConstant.ORDERLINE, key_prefix_ORDERLINE, columns, null, null, null, false);

		for (Map<String, Result> list : results) {
			ol_number = new String(list.get("ol_number").getValue());
			ol_total += Float.valueOf(new String(list.get("ol_amount").getValue())); 
			String key_ORDERLINE = (TPCCUtil.buildString(w_id, "_", d_id, "_", no_o_id, "_", ol_number));

			List<String> columns_neworder = TPCCUtil.buildColumns("ol_delivery_d");
			List<String> values_neworder = TPCCUtil.buildColumns(ol_delivery_d);
			t.write(TPCCConstant.ORDERLINE, key_ORDERLINE, columns_neworder, values_neworder, 1);
		}

		/* retrieve balance of customers and c_delivery_cnt*/
		float c_balance =  0.0f;
		int c_delivery_cnt = 0;

		columns = TPCCUtil.buildColumns("c_balance", "c_delivery_cnt");
		result = t.doRead(TPCCConstant.CUSTOMER, key_customer, columns);

		c_balance = Float.valueOf(new String(result.get("c_balance").getValue()));
		c_delivery_cnt = Integer.valueOf(new String(result.get("c_delivery_cnt").getValue()));

		/* update c_balance, c_delivery_cnt of customers */
		columns = TPCCUtil.buildColumns("c_balance", "c_delivery_cnt");
		values = TPCCUtil.buildColumns(c_balance + ol_total, c_delivery_cnt + 1);
		t.write(TPCCConstant.CUSTOMER, key_customer, columns, values, 1);


		t.commit();

		//lock_delivery.set(false);
		
		if (!flag_output) {
			return;
		}
		System.out.println("==============================Delivery==================================");
		System.out.println("INPUT	o_carrier_id: " + o_carrier_id);
		System.out.println();
		System.out.println("TPCCConstant.WAREHOUSE: " + w_id);
		System.out.println("o_carrier_id: " + o_carrier_id);
		System.out.println("Execution Status: Delivery has been queued");
		System.out.println("=========================================================================");

		
		
	}
	
	
	/*
	 * Function name: Stocklevel
	 * Description: The Stock-Level business transaction determines the number of recently
	 * 				sold items that have a stock level below a specified threshold.
	 * Argument: threshold - TPCCGenerator.randomInt(10, 20)
	 */
	public void Stocklevel(int threshold) throws TransactionException {

		MDCCTransaction t = fac.create();
		t.begin();
		
		/* local variables */
		int w_id = TPCCUtil.randomInt(1, COUNT_WARE);
		int d_id = TPCCUtil.randomInt(1, TPCCConstant.DIST_PER_WARE);
		Map<String, Result> result;
		List<String> columns;

		/* retrieve district d_next_o_id  */
		int d_next_o_id = 0;
		columns = TPCCUtil.buildColumns("d_next_o_id");
		String key_district = TPCCUtil.buildString(w_id, "_", d_id);
		result = t.doRead(TPCCConstant.DISTRICT, key_district, columns);
		d_next_o_id = Integer.valueOf(new String(result.get("d_next_o_id").getValue()));
	
		/* retrieve order_line information  */
		int ol_i_id = 0;
		int low_stock = 0;
		/*
		 * All rows in the ORDER-LINE table with matching OL_W_ID (equals W_ID), OL_D_ID (equals D_ID), and
		 * OL_O_ID (lower than D_NEXT_O_ID and greater than or equal to D_NEXT_O_ID minus 20) are selected.
		 * They are the items for 20 recent orders of the district.
		 */
		String key_prefix_order = (TPCCUtil.buildString(w_id, "_", d_id));
		List<Result> _result = t.doRead(TPCCConstant.ORDERLINE, key_prefix_order, "ol_i_id", "ol_o_id", d_next_o_id - 20, d_next_o_id);
		/*
		 * All rows in the STOCK table with matching S_I_ID (equals OL_I_ID) and S_W_ID (equals W_ID) from
		 * the list of distinct item numbers and with S_QUANTITY lower than threshold are counted (giving low_stock).
		 */
		Set<Integer> set = new HashSet<Integer>();
		for (Result b : _result) {
			ol_i_id = Integer.valueOf(new String(b.getValue()));
			if (set.add(ol_i_id)) {
				String key_prefix_stock = TPCCUtil.buildString(w_id, "_", ol_i_id);
				low_stock = t.doRead(TPCCConstant.STOCK, key_prefix_stock, "s_quantity", -1,  threshold);
			} 
		}
		

		t.commit();

		
		if (!flag_output) {
			return;
		}
		System.out.println("==============================Stock Level================================");
		System.out.println("INPUT	threshold: " + threshold);
		System.out.println();
		System.out.println("WAREHOUSE: " + w_id + "\tDistrict: " + d_id);
		System.out.println("Stock Level Threshold: " + threshold);
		System.out.println("low stock: " + low_stock);
		System.out.println("=========================================================================");

	}
	
	
}
