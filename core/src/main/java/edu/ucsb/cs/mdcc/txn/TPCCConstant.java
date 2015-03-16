package edu.ucsb.cs.mdcc.txn;

public class TPCCConstant {
	/* The constants as specified */
	public static final int MAXITEMS 		= 100000;
	public static final int CUST_PER_DIST 	= 3000;
	public static final int DIST_PER_WARE	= 10;
	public static final int ORD_PER_DIST 	= 3000;
	/* NURand */
	public static final int A_C_LAST 	= 255;
	public static final int A_C_ID 		= 1023;
	public static final int A_OL_I_ID 	= 8191;
	public static final int C_C_LAST 	= TPCCUtil.randomInt(0, A_C_LAST);
	public static final int C_C_ID 		= TPCCUtil.randomInt(0, A_C_ID);
	public static final int C_OL_I_ID 	= TPCCUtil.randomInt(0, A_OL_I_ID);
	/* constant names of the column families */
	public static String WAREHOUSE 	= 	"warehouse";
	public static String DISTRICT 	= 	"district";
	public static String CUSTOMER 	= 	"customer";
	public static String HISTORY 	= 	"history";
	public static String ORDER 		= 	"order";
	public static String NEWORDER 	= 	"new_order";
	public static String ORDERLINE 	= 	"order_line";
	public static String ITEM 		= 	"item";
	public static String STOCK 		= 	"stock";
}
