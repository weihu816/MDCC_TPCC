package edu.ucsb.cs.mdcc.dao;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import edu.ucsb.cs.mdcc.config.TPCCConfiguration;

public class Loader {

	public InMemoryDatabase db;
	
	/* Number of warehouses */
	private int count_ware = TPCCConfiguration.getConfiguration().getNum_warehouse(); 
	/* The constants as specified */
	public static final int MAXITEMS = 100000;
	public static final int CUST_PER_DIST = 3000;
	public static final int DIST_PER_WARE = 10;
	public static final int ORD_PER_DIST = 3000;
	/* NURand */
	public static final int A_C_LAST = 255;
	public static final int A_C_ID = 1023;
	public static final int A_OL_I_ID = 8191;
	public static final int C_C_LAST = randomInt(0, A_C_LAST);
	public static final int C_C_ID = randomInt(0, A_C_ID);
	public static final int C_OL_I_ID = randomInt(0, A_OL_I_ID);
	/* constant names of the column families */
	public static String WAREHOUSE = "warehouse";
	public static String DISTRICT = "district";
	public static String CUSTOMER = "customer";
	public static String HISTORY = "history";
	public static String ORDER = "order";
	public static String NEWORDER = "new_order";
	public static String ORDERLINE = "order_line";
	public static String ITEM = "item";
	public static String STOCK = "stock";
	/* Encoding */
	public static String UTF8 = "UTF-8";

	
	public void load() {
		LoadItems();
		LoadWare();
		LoadCust();
		LoadOrd();
	}

	public Loader (InMemoryDatabase db) {
		this.db = db;
	}
	
	public Loader(InMemoryDatabase db, int w) {
		this.db = db;
		this.count_ware = w;
	}
	
	public String MakeAlphaString(int min, int max) {
		StringBuffer str = new StringBuffer();
		Random random = new Random();
		long result = 0;
		int number = random.nextInt(max) % (max-min+1) + min;
		for (int i = 0; i < number; i++) {
			switch (random.nextInt(3)) {
			case 0: // CAP letter
				result = Math.round(Math.random() * 25 + 65);
				str.append(String.valueOf((char) result));
				break;
			case 1: // Low letter
				result = Math.round(Math.random() * 25 + 97);
				str.append(String.valueOf((char) result));
				break;
			case 2: // Number
				str.append(String.valueOf(new Random().nextInt(10)));
				break;
			}
		}
		return str.toString();
	}
	
	public String MakeNumberString(int min, int max) {
		StringBuffer str = new StringBuffer();
		Random random = new Random();
		int number = random.nextInt(max + 1)%(max-min+1) + min;
		for (int i = 0; i < number; i++) {
			str.append(String.valueOf(new Random().nextInt(10)));
		}
		return str.toString();
	}

	public static int NURand(int A, int x, int y) {
		int c = 0;
		switch (A) {
		case A_C_LAST:
			c = C_C_LAST;
			break;
		case A_C_ID:
			c = C_C_ID;
			break;
		case A_OL_I_ID:
			c = C_OL_I_ID;
			break;
		default:
		}
		return (((randomInt(0, A) | randomInt(x, y)) + c) % (y - x + 1)) + x;
	}

	public static int randomInt(int min, int max) {
		return new Random().nextInt(max + 1) % (max - min + 1) + min;
	}
	
	public static float randomFloat(float min, float max) {
		return new Random().nextFloat() * (max - min) + min;
	}
	
	public String buildString(Object ... args) {
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < args.length; i++) {
			if (args[i] instanceof String) {
				stringBuffer.append((String)args[i]);
			} else {
				stringBuffer.append(String.valueOf(args[i]));
			}
			
		}
		return stringBuffer.toString();
	}
	
	public String[] buildColumns(Object ... args) {
		String[] columns = new String[args.length];
		for (int i = 0; i < args.length; i++) {
			if (args[i] instanceof String) {
				columns[i] = (String)args[i];
			} else {
				columns[i] = String.valueOf(args[i]);
			}
		}
		return columns;
	}
	
	public Record[] buildRecords(String table, String key, String[] columns, Object ... args) {
		Record[] ret = new Record[args.length];
		for (int i = 0; i < args.length; i++) {
			String value = "";
			if (args[i] instanceof String) {
				value = (String)args[i];
			} else {
				value = String.valueOf(args[i]);
			}
			Record r = new Record(buildString(table, ":", key, ":", columns[i]));
			r.setValue(value.getBytes());
			ret[i] = r;
		}
		return ret;
	}

	/*
	 * Description: This function Load items into item table
	 */
	public void LoadItems() {

		/* local varibales */
		int i_id;
		String i_name;
		float i_price;
		String i_data;

		int idatasiz;
		int orig[] = new int[MAXITEMS];
		int pos = 0;
		
		/* column family */;
		System.out.println("Loading Item");

		/* random of 10% items that will be marked 'original ' */
		for (int i = 0; i < MAXITEMS; i++) { orig[i] = 0; }
		for (int i = 0; i < MAXITEMS / 10; i++) {
			do { pos = (new Random()).nextInt(MAXITEMS); } while (orig[pos] == 1);
			orig[pos] = 1;
		}

		for (i_id = 1; i_id <= MAXITEMS; i_id++) {
			/* Generate Item Data */
			i_name = MakeAlphaString(14, 24);
			i_price = randomFloat(100, 10000) / 100.0f;
			i_data = MakeAlphaString(26, 50);
			idatasiz = i_data.length();
			
			if (orig[i_id - 1] == 1) {
				pos = randomInt(0, idatasiz - 8);
				i_data = i_data.substring(0, pos) + "original" + i_data.substring(pos+8);
			}
			
			/* key */
			String key = String.valueOf(i_id);
			/* column */
			String[] columns = buildColumns("i_id", "i_name", "i_price", "i_data");
			Record[] values = buildRecords("item", key, columns, i_id, i_name, i_price, i_data);
			db.insert(ITEM, key, columns, values);

			if( i_id % 100 == 0 ){
				System.out.print(".");
				if ( i_id % 5000 == 0 ) System.out.println(i_id);
			}
		}
		System.out.println("Item Done.");
	}

	/*
	 * Function name: LoadWare
	 * Description: Load the stock table, then call Stock and District
	 * Argument: none
	 */
	public void LoadWare() {
		int w_id;
		String w_name;
		String w_street_1;
		String w_street_2; 
		String w_city; 
		String w_state; 
		String w_zip;
		float w_tax;
		float w_ytd;
		
		/* start loading */
		System.out.println("Loading Warehouses");
		
		for (w_id = 1; w_id <= count_ware; w_id++) {
			
			/* Generate Warehouse Data */
			w_name = MakeAlphaString( 6, 10 );
			w_street_1 = MakeAlphaString( 10,20); 	/* Street 1 */
			w_street_2 = MakeAlphaString( 10,20 ); 	/* Street 2 */ 
			w_city = MakeAlphaString( 10,20 ); 		/* City */
			w_state = MakeAlphaString( 2,2 ); 		/* State */
			w_zip = MakeNumberString( 9,9 ); 		/* Zip */
			w_tax= randomFloat( 10,20 ) / 100.0f;
			w_ytd = 3000000.0f;
			
			
			/* key */
			String key = String.valueOf(w_id);
			/* column */
			String[] columns = buildColumns("w_id", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip", "w_tax", "w_ytd");
			Record[] values = buildRecords("warehouse", key, columns, w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd);
			db.insert(WAREHOUSE, key, columns, values);

			/* Make rows associated with warehouse*/
			Stock(w_id);
			District(w_id);
			
		}
		
	}
	
	/*
	 * Function name: Stock
	 * Description: Load the stock table
	 * Argument: w_id - warehouse id
	 */
	void Stock(int w_id) {
		
		/* local varibales */
		int s_i_id;
		int s_w_id;
		int s_quantity;
		String s_data;
		
		int sdatasiz;
		int orig[] = new int[MAXITEMS];
		int pos;

		/* Starting Loading ...*/
		System.out.println("Loading Stock Wid = " + w_id);
		s_w_id = w_id;
		
		for (int i=0; i < MAXITEMS; i++) {
			orig[i] = 0; 
		}
		
		for (int i = 0; i < MAXITEMS / 10; i++)
		{
			do {
				pos = (new Random()).nextInt(MAXITEMS);
			} while(orig[pos] == 1);
			orig[pos] = 1;
		}
		
		for (s_i_id = 1; s_i_id <= MAXITEMS; s_i_id++) {
			/* Generate Stock Data */
			s_quantity= randomInt(10,100);
			s_data = MakeAlphaString(26,50);
			sdatasiz = s_data.length();
			if (orig[s_i_id-1] == 1) { 
				pos = randomInt(0, sdatasiz - 8); 
				s_data = s_data.substring(0, pos) + "original" + s_data.substring(pos+8);
			}
			
			/* key */
			String key = buildString(s_w_id, "_", s_i_id);
			/* column */
			String[] columns = buildColumns("s_i_id", "s_w_id", "s_quantity",
					"s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04",
					"s_dist_05", "s_dist_06", "s_dist_07", "s_dist_08",
					"s_dist_09", "s_dist_10", "s_data", "s_ytd", "s_order_cnt",
					"s_remote_cnt");
			Record[] values = buildRecords("stock", key, columns, s_i_id, s_w_id, s_quantity,
					MakeAlphaString(24, 24), MakeAlphaString(24, 24),
					MakeAlphaString(24, 24), MakeAlphaString(24, 24),
					MakeAlphaString(24, 24), MakeAlphaString(24, 24),
					MakeAlphaString(24, 24), MakeAlphaString(24, 24),
					MakeAlphaString(24, 24), MakeAlphaString(24, 24), s_data,
					"0", "0", "0");
			db.insert(STOCK, key, columns, values);

			if ( (s_i_id % 100) == 0 ) {
				System.out.print(".");
				if ( s_i_id % 5000 == 0) System.out.println(s_i_id);
			}
		}
		
		System.out.println("Stock Done.");

	}

	/*
	 * Function name: District
	 * Description: Load the district table
	 * Argument: w_id - warehouse id
	 */
	void District(int w_id) {
		/* local varibales */
		int d_id, d_w_id;
		String d_name, d_street_1, d_street_2, d_city, d_state, d_zip;
		float d_tax, d_ytd;
		int d_next_o_id;
		
		/* Starting Loading ...*/
		System.out.println("Loading District Wid = " + w_id);
		
		d_w_id = w_id; 
		d_ytd = 30000.0f; 
		d_next_o_id = 3001;
		
		for (d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
			/* Generate District Data */ 
			d_name = MakeAlphaString(6,10);
			d_street_1 = MakeAlphaString( 10,20); 	/* Street 1 */
			d_street_2 = MakeAlphaString( 10,20 ); 	/* Street 2 */ 
			d_city = MakeAlphaString( 10,20 ); 		/* City */
			d_state = MakeAlphaString( 2,2 ); 		/* State */
			d_zip = MakeNumberString( 9,9 ); 		/*Zip */
			d_tax=(randomFloat(10,20)) / 100.0f;

			/* key */
			String key = buildString(d_w_id, "_", d_id);
			/* column */
			String[] columns = buildColumns("d_id", "d_w_id", "d_name",
					"d_street_1", "d_street_2", "d_city", "d_state", "d_zip",
					"d_tax", "d_ytd", "d_next_o_id");
			Record[] values = buildRecords("district", key, columns, d_id, d_w_id, d_name, d_street_1,
					d_street_2, d_city, d_state, d_zip, d_tax, d_ytd,
					d_next_o_id);
			db.insert(DISTRICT, key, columns, values);
		}
		
		System.out.println("District Done.");

	}
	
	/*
	 * Function name: LoadCust
	 * Description: Call Customer() to load the Customer table
	 * Argument: none
	 */
	public void LoadCust() {

		for (int w_id = 1; w_id<=count_ware; w_id++) {
			for (int d_id = 1; d_id<=DIST_PER_WARE; d_id++) {
				Customer(d_id, w_id);
			}
		}
		System.out.println("Customer Done.");

	}

	/*
	 * Function name: LoadCust
	 * Description: Load the customer table, the histroy table
	 * Argument: none
	 */
	void Customer(int d_id, int w_id) {
		int	c_id, c_d_id, c_w_id, c_credit_lim;
		String c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip;
		String c_phone, c_since, c_credit, c_data, h_date, h_data; 
		float c_discount, c_balance, h_amount; 
		
		/* Already Set up database connection */
		
		for (c_id = 1; c_id <= CUST_PER_DIST; c_id ++) {
			/* Generate Customer Data */
			c_d_id = d_id;
			c_w_id = w_id;
			c_first = MakeAlphaString( 8, 16 ); 
			c_middle = "OE";
			if (c_id <= 1000) { 
				c_last = Lastname(c_id - 1);
			} else {
				c_last = Lastname(NURand(A_C_LAST, 0, 999));
			}
			c_street_1 = MakeAlphaString( 10,20); 	/* Street 1 */
			c_street_2 = MakeAlphaString( 10,20 ); 	/* Street 2 */ 
			c_city = MakeAlphaString( 10,20 ); 		/* City */
			c_state = MakeAlphaString( 2,2 ); 		/* State */
			c_zip = MakeNumberString( 9,9 ); 		/* Zip */
			c_since = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
			c_phone = MakeNumberString(16, 16);
			if ( randomInt( 0, 1 ) == 1) {
				c_credit = "GC";
			} else {
				c_credit = "BC";
			}
			c_credit_lim = 50000;
			c_discount = (randomFloat(0, 50)) / 100.0f;
			c_balance = -10.0f;
			c_data = MakeAlphaString(300, 500);
			
			/* Insert into database */
			
			/* key */
			String key = buildString(c_w_id, "_",  c_d_id, "_", c_id);
			/* insert into the customer table */
			String[] columns = buildColumns("c_id", "c_d_id", "c_w_id",
					"c_first", "c_middle", "c_last", "c_street_1",
					"c_street_2", "c_city", "c_state", "c_zip", "c_phone",
					"c_since", "c_credit", "c_credit_lim", "c_discount",
					"c_balance", "c_ytd_payment", "c_payment_cnt",
					"c_delivery_cnt", "c_data");
			Record[] values = buildRecords("customer", key, columns, c_id, c_d_id, c_w_id, c_first,
					c_middle, c_last, c_street_1, c_street_2, c_city, c_state,
					c_zip, c_phone, c_since, c_credit, c_credit_lim,
					c_discount, c_balance, 10.0f, 1, 0, c_data);
			db.insert(CUSTOMER, key, columns, values);
			
			
			
			/* history table data generation */
			h_amount = 10.0f; 
			h_date = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
			h_data = MakeAlphaString( 12,24 );
			
			/* key */
			key = String.valueOf(System.currentTimeMillis());
			/* insert into the history table */
			String[] columns_h = buildColumns("h_c_id", "h_c_d_id", "h_c_w_id", "h_w_id",
					"h_d_id", "h_date", "h_amount", "h_data");
			Record[] values_h = buildRecords("customer", key, columns, c_id, c_d_id, c_w_id, c_w_id,
					c_d_id, h_date, h_amount, h_data);
			db.insert(HISTORY, key, columns_h, values_h);

			if (c_id % 100 == 0) {
				System.out.print(".");
				if (c_id % 1000 == 0) System.out.println(c_id);
			}
		}
		

	}
	
	/*
	 * Function name: LoadOrd
	 * Description: Call Orders() to load the order table and new_order table
	 * Argument: none
	 */
	public void LoadOrd() {
		for (int w_id = 1; w_id<=count_ware; w_id++) {
			for (int d_id = 1; d_id<=DIST_PER_WARE; d_id++) {
				Orders(d_id, w_id);
			}
		}
		System.out.println( "Order Done.");
	}

	/*
	 * Function name: Orders
	 * Description: Loads the order table as well as the order_line table
	 * Argument: d_id - district id
	 * 			 w_id - warehouse id
	 */
	void Orders( int d_id, int w_id ){
		int o_id, o_c_id, o_d_id, o_w_id, o_carrier_id, o_ol_cnt;
		int ol, ol_i_id, ol_supply_w_id, ol_quantity;
		String o_entry_d, ol_dist_info, ol_delivery_d; 
		float ol_amount;
				
		o_d_id = d_id;
		o_w_id = w_id;
		o_entry_d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		ol_delivery_d = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(new Date(System.currentTimeMillis()));
		/* initialize permutation of customer numbers */
		int permutation[] = new int[CUST_PER_DIST];
		for (int i = 0; i < CUST_PER_DIST; i++) {
			permutation[i] = i + 1;
		}
		for (int i = 0; i < permutation.length; i++) {
			int index = (new Random()).nextInt(CUST_PER_DIST);
			int t = permutation[i];
			permutation[i] = permutation[index];
			permutation[index] = t;
		}
		
		for (o_id = 1; o_id <= ORD_PER_DIST; o_id++) {
			/* Generate Order Data */ 
			o_c_id = permutation[o_id - 1];
			o_carrier_id = randomInt( 1, 10 );
			o_ol_cnt = randomInt( 5, 15 );
			
			/* key */
			String key = buildString(o_w_id, "_", o_d_id, "_", o_id);
			/* insert into the customer table */
			String[] columns;
			Record[] values; 
			
			
			if (o_id > 2100) { 
				/* the last 900 orders have not been delivered */
				columns = buildColumns("o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_ol_cnt", "o_all_local", "o_carrier_id");
				values = buildRecords("order", key, columns, o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, 1, "NULL");
				db.insert(ORDER, key, columns, values);
				/* Load new order table */
				New_Orders( o_id, o_w_id, o_d_id );
				
			} else {
				/* the first 2100 orders have not been delivered */
				columns = buildColumns("o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_ol_cnt", "o_all_local", "o_carrier_id");
				values = buildRecords("order", key, columns, o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, 1, o_carrier_id);
				db.insert(ORDER, key, columns, values);
			}
			
			
			for (ol = 1; ol <= o_ol_cnt; ol++) {
				
				String key_orderline = buildString(o_w_id, "_", o_d_id, "_", o_id, "_", ol);
				
				/* Generate Order Line Data */
				ol_i_id = randomInt(1, MAXITEMS);
				ol_supply_w_id = o_w_id;
				ol_quantity = 5;
				ol_amount = randomFloat(10, 10000) / 100.0f; /* randomly generated in specification */
				ol_dist_info = MakeAlphaString( 24,24 );
				

				columns = buildColumns("ol_o_id", "ol_d_id", "ol_w_id",
						"ol_number", "ol_i_id", "ol_supply_w_id",
						"ol_quantity", "ol_dist_info", "ol_amount",
						"ol_delivery_d");
				values = buildRecords("order", key, columns, o_id, o_d_id, o_w_id, ol, ol_i_id,
						ol_supply_w_id, ol_quantity, ol_dist_info,
						ol_amount, ol_delivery_d);
				db.insert(ORDERLINE, key_orderline, columns, values);

			}
			
			if ( o_id % 100 == 0 ){
				System.out.print(".");
				if ( o_id % 1000 == 0 ) System.out.println( o_id );
			}
		}
		
	}
	
	/*
	 * Function name: New_Orders
	 * Description:
	 * Argument: none
	 */
	void New_Orders(int o_id, int no_w_id, int no_d_id) {
		String key = buildString(no_w_id, "_", no_d_id, "_", o_id);
		String[] columns = buildColumns("no_o_id", "no_w_id", "no_d_id");
		Record[] values = buildRecords("order", key, columns, o_id, no_w_id, no_d_id);
		db.insert(NEWORDER, key, columns, values);
	}

	/*
	 * This function generates the last name for customers
	 * Argument : int num, String name
	 */
	public static String Lastname(int num) {
		String name = "";
		String n[] = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
		name += n[num / 100];
		name += n[(num / 10) % 10];
		name += n[num % 10];
		return name;
	}

}