package edu.ucsb.cs.mdcc.txn;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TPCCUtil {

	static Random random = new Random();

	/*
	 * Description: This function return next random integer within the range
	 */
	public static int randomInt(int min, int max) {
		return random.nextInt(max + 1) % (max - min + 1) + min;
	}

	/*
	 * Description: This function return next random float within the range
	 */
	public static float randomFloat(float min, float max) {
		return random.nextFloat() * (max - min) + min;
	}

	/*
	 * Description: This function return generated NR random number
	 */
	public static int NURand(int A, int x, int y) {
		int c = 0;
		switch (A) {
		case TPCCConstant.A_C_LAST:
			c = TPCCConstant.C_C_LAST;
			break;
		case TPCCConstant.A_C_ID:
			c = TPCCConstant.C_C_ID;
			break;
		case TPCCConstant.A_OL_I_ID:
			c = TPCCConstant.C_OL_I_ID;
			break;
		default:
		}
		return (((randomInt(0, A) | randomInt(x, y)) + c) % (y - x + 1)) + x;
	}

	/*
	 * The random name generation strategy
	 */
	public static String Lastname(int num) {
		String name = "";
		String n[] = { "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI",
				"CALLY", "ATION", "EING" };
		name += n[num / 100];
		name += n[(num / 10) % 10];
		name += n[num % 10];
		return name;
	}

	public static String buildString(Object... args) {
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < args.length; i++) {
			if (args[i] instanceof String) {
				stringBuffer.append((String) args[i]);
			} else {
				stringBuffer.append(String.valueOf(args[i]));
			}

		}
		return stringBuffer.toString();
	}

	public static List<String> buildColumns(Object... args) {
		List<String> columns = new ArrayList<String>();
		for (int i = 0; i < args.length; i++) {
			if (args[i] instanceof String) {
				columns.add((String) args[i]);
			} else {
				columns.add(String.valueOf(args[i]));
			}
		}
		return columns;
	}
}
