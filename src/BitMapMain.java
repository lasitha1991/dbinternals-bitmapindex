import java.util.Dictionary;
import java.util.Hashtable;
import java.sql.ResultSet;
import java.io.*;

public class BitMapMain {

	/**
	 * @param args
	 *            global variables should be assigned to BitMapIndexGenerator
	 */
	/**
	 * INSERT TEST int TEST_SIZE = 100; String[] possibleValues = new
	 * String[]{"Young", "Middle", "Old"}; SQLconnection sqlconnection = new
	 * SQLconnection("chamara","123"); long startTime =
	 * System.currentTimeMillis(); for (int counter = 0 ; counter < TEST_SIZE;
	 * counter++){
	 * sqlconnection.insert("INSERT INTO Workers values (default,?,?)", new
	 * String [] {"Cham"+counter,possibleValues[counter%3]}); } long endTime =
	 * System.currentTimeMillis(); long time_for_execution = endTime -
	 * startTime;
	 * System.out.println("time_for_execution = "+time_for_execution);
	 * 
	 * 
	 * **/

	private final int iterations = 5 ;
	private final int cardinality = 100 ;
	private static int[] mode = {0,1 }; // /0 for non bit map, 1 for binning , 2
										// for log encoded

	private static int[] sizes = new int[] { 1000 };//,2000, 5000, 10000};//, 20000,
																	// 50000,
											// 100000, 200000, 500000, 1000000};
	
	
	SQLconnection sqlconnection;
	LogEncodedBitmapSQLconnection logSqlConnection;

	public BitMapMain() {
		sqlconnection = new SQLconnection("root", "");
		logSqlConnection = new LogEncodedBitmapSQLconnection("root", "");
	}

	public static void main(String[] args) {
		BitMapMain main = new BitMapMain();
		
		for (int size : sizes) {
			main.executeInsert(10000);
			for (int m : mode) {				
				if (m == 0) { //non bitmap
//					main.executeNonBitmapInsert(size);
					main.executeNonBitmapRead(size);
				}				
				if (m == 1) { //binning
//					main.executeInsert(size);
					main.executeRead(size);
				}
				if (m == 2) { //log encoding
					main.executeLogEncodedInsert(size);
					main.executeLogEncodedRead(size);
				}
			}
		}
	}

	public void executeInsert(int testSize) {
		System.out.println("bin starting insert");
//		int[] initialBitmapSizes = new int[] { 15, 30, 150, 300, 450, 1500,
//				3000 };
//		int[] bitmapExpansionSizes = new int[] { 10, 20, 100, 200, 300, 1000,
//				2000 };
		// for (int TEST_SIZE : sizes) {
		// for (int j = 0; j < 5; j++) {
		// System.out.println("executing for bitmap initial size:"
		// + initialBitmapSizes[j] + " bitmap expantion size: "
		// + bitmapExpansionSizes[j]);
		String[] possibleValues = new String[] { "Colombo", "Hambanthota",
				"Kandy", "Jaffna", "Galle" };

		// sqlconnection.resetBitmapIndex(initialBitmapSizes[j],
		// bitmapExpansionSizes[j]);
		sqlconnection.ExecuteQuery("DROP TABLE Workers");
		sqlconnection
				.ExecuteQuery("CREATE TABLE Workers (id int(11) NOT NULL AUTO_INCREMENT, name varchar(20) DEFAULT NULL,work_site varchar(20) DEFAULT NULL,PRIMARY KEY (id));");
		// sqlconnection.ExecuteQuery("ALTER TABLE `workers` ADD INDEX ( `id` ) ");

		double[] runtimes = new double[iterations];
		for (int i = 0; i < 1; i++) {
			long startTime = System.currentTimeMillis();
			for (int counter = 0; counter < testSize; counter++) {
				sqlconnection.insert(
						"INSERT INTO Workers values (default,?,?)",
						new String[] {
								"Cham" + counter,
								(possibleValues[counter % 5] + counter
										% cardinality) });
			}
			long endTime = System.currentTimeMillis();
			long time_for_execution = endTime - startTime;
			try {
				File file = new File("output_insert.txt");
				if (!file.exists()) {
					file.createNewFile();
				}
				FileWriter filewriter = new FileWriter(file.getAbsoluteFile(),
						true);
				BufferedWriter bufferedWriter = new BufferedWriter(filewriter);
				bufferedWriter.write("\nTEST_SIZE = " + testSize
						+ "    time_for_execution = " + time_for_execution);
				System.out.println(testSize + "    time_for_execution = "
						+ time_for_execution);
				// sqlconnection.print();
				runtimes[i] = time_for_execution;
				if (i == iterations - 1) {
					double total = 0;
					for (double value : runtimes) {
						total += value;
					}
					bufferedWriter.write("\nTEST_SIZE = " + testSize
							+ "    average_time_for_execution = " + total
							/ iterations);
					System.out.println(testSize
							+ "    average_time_for_execution = " + total
							/ iterations);
				}
				bufferedWriter.close();
				// sqlconnection.read("select * from workers where id=? and age_group=?",new
				// String[]{"Old"});
			} catch (Exception e) {
				// }
				// }
			}
		}
		System.out.println("bin insert Execution finished");

		// TODO Auto-generated method stub
		// DataReader data_reader = new DataReader("");
		/*
		 * @SuppressWarnings("unchecked") BitMapIndexGenerator index = new
		 * BitMapIndexGenerator(data_reader.Read_Data(),1000, 500) ;
		 */

	}

	public void executeLogEncodedInsert(int testSize) {
		System.out.println("log starting insert");
		int[] initialBitmapSizes = new int[] { 15, 30, 150, 300, 450, 1500,
				3000 };
		int[] bitmapExpansionSizes = new int[] { 10, 20, 100, 200, 300, 1000,
				2000 };
		// for (int TEST_SIZE : sizes) {
		// for (int j = 0; j < 5; j++) {
		// System.out.println("executing for bitmap initial size:"
		// + initialBitmapSizes[j] + " bitmap expantion size: "
		// + bitmapExpansionSizes[j]);
		String[] possibleValues = new String[] { "Colombo", "Hambanthota",
				"Kandy", "Jaffna", "Galle" };

		// sqlconnection.resetBitmapIndex(initialBitmapSizes[j],
		// bitmapExpansionSizes[j]);
		logSqlConnection.ExecuteQuery("DROP TABLE Workers");
		logSqlConnection
				.ExecuteQuery("CREATE TABLE Workers (id int(11) NOT NULL AUTO_INCREMENT, name varchar(20) DEFAULT NULL,work_site varchar(20) DEFAULT NULL,PRIMARY KEY (id));");
		// sqlconnection.ExecuteQuery("ALTER TABLE `workers` ADD INDEX ( `id` ) ");

		double[] runtimes = new double[iterations];
		for (int i = 0; i < iterations; i++) {
			long startTime = System.currentTimeMillis();
			for (int counter = 0; counter < testSize; counter++) {
				logSqlConnection.insert(
						"INSERT INTO Workers values (default,?,?)",
						new String[] {
								"Cham" + counter,
								(possibleValues[counter % 5] + counter
										% cardinality) });
			}
			long endTime = System.currentTimeMillis();
			long time_for_execution = endTime - startTime;
			try {
				File file = new File("output_log_insert.txt");
				if (!file.exists()) {
					file.createNewFile();
				}
				FileWriter filewriter = new FileWriter(file.getAbsoluteFile(),
						true);
				BufferedWriter bufferedWriter = new BufferedWriter(filewriter);
				bufferedWriter.write("\nTEST_SIZE = " + testSize
						+ "    time_for_execution = " + time_for_execution);
				System.out.println(testSize + "    time_for_execution = "
						+ time_for_execution);
				// sqlconnection.print();
				runtimes[i] = time_for_execution;
				if (i == iterations - 1) {
					double total = 0;
					for (double value : runtimes) {
						total += value;
					}
					bufferedWriter.write("\nTEST_SIZE = " + testSize
							+ "    average_time_for_execution = " + total
							/ iterations);
					System.out.println(testSize
							+ "    average_time_for_execution = " + total
							/ iterations);
				}
				bufferedWriter.close();
				// sqlconnection.read("select * from workers where id=? and age_group=?",new
				// String[]{"Old"});
			} catch (Exception e) {
				// }
				// }
			}
		}
		System.out.println("log insert Execution finished");

		// TODO Auto-generated method stub
		// DataReader data_reader = new DataReader("");
		/*
		 * @SuppressWarnings("unchecked") BitMapIndexGenerator index = new
		 * BitMapIndexGenerator(data_reader.Read_Data(),1000, 500) ;
		 */

	}

	public void executeRead(int testSize) {
		System.out.println("bin read started");
		String[] possibleValues = new String[] { "Colombo", "Hambanthota",
				"Kandy", "Jaffna", "Galle" };
		double[] runtimes = new double[iterations];
		for (int i = 0; i < iterations; i++) {
			long startTime = System.currentTimeMillis();
			for (int counter = 0; counter < testSize; counter++) {
				sqlconnection.read(
						"select * from workers where # and work_site=?",//
						new String[] { possibleValues[counter % 5] + counter
								% 7 });
			}
			long endTime = System.currentTimeMillis();
			long time_for_execution = endTime - startTime;
			try {
				File file = new File("output_read.txt");
				if (!file.exists()) {
					file.createNewFile();
				}
				FileWriter filewriter = new FileWriter(file.getAbsoluteFile(),
						true);
				BufferedWriter bufferedWriter = new BufferedWriter(filewriter);
				bufferedWriter.write("\nTEST_SIZE = " + testSize
						+ "    time_for_execution = " + time_for_execution);
				System.out.println(testSize + "    time_for_execution = "
						+ time_for_execution);
				// sqlconnection.print();
				runtimes[i] = time_for_execution;
				if (i == iterations - 1) {
					double total = 0;
					for (double value : runtimes) {
						total += value;
					}
					bufferedWriter.write("\nTEST_SIZE = " + testSize
							+ "    average_time_for_execution = " + total
							/ iterations);
					System.out.println(testSize
							+ "    average_time_for_execution = " + total
							/ iterations);
				}
				bufferedWriter.close();

			} catch (Exception e) {
				// }
				// }
			}
		}
		System.out.println("bin read Execution finished");
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {

		}

	}

	public void executeLogEncodedRead(int testSize) {
		System.out.println("log read started");
		String[] possibleValues = new String[] { "Colombo", "Hambanthota",
				"Kandy", "Jaffna", "Galle" };
		double[] runtimes = new double[iterations];
		for (int i = 0; i < iterations; i++) {
			long startTime = System.currentTimeMillis();
			for (int counter = 0; counter < testSize; counter++) {
				logSqlConnection.read(
						"select * from workers where #",//and work_site=?
						new String[] { possibleValues[counter % 5] + counter
								% 7 });
			}
			long endTime = System.currentTimeMillis();
			long time_for_execution = endTime - startTime;
			try {
				File file = new File("output_log_read.txt");
				if (!file.exists()) {
					file.createNewFile();
				}
				FileWriter filewriter = new FileWriter(file.getAbsoluteFile(),
						true);
				BufferedWriter bufferedWriter = new BufferedWriter(filewriter);
				bufferedWriter.write("\nTEST_SIZE = " + testSize
						+ "    time_for_execution = " + time_for_execution);
				System.out.println(testSize + "    time_for_execution = "
						+ time_for_execution);
				// sqlconnection.print();
				runtimes[i] = time_for_execution;
				if (i == iterations - 1) {
					double total = 0;
					for (double value : runtimes) {
						total += value;
					}
					bufferedWriter.write("\nTEST_SIZE = " + testSize
							+ "    average_time_for_execution = " + total
							/ iterations);
					System.out.println(testSize
							+ "    average_time_for_execution = " + total
							/ iterations);
				}
				bufferedWriter.close();

			} catch (Exception e) {
				// }
				// }
			}
		}
		System.out.println("log read Execution finished");
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {

		}

	}

	public void executeNonBitmapRead(int testSize) {
		System.out.println("non bitmap read started");
		String[] possibleValues = new String[] { "Colombo", "Hambanthota",
				"Kandy", "Jaffna", "Galle" };
		double[] runtimes = new double[iterations];
		for (int i = 0; i < iterations; i++) {
			long startTime = System.currentTimeMillis();
			for (int counter = 0; counter < testSize; counter++) {
				sqlconnection
						.ExecuteNonBitmapRead("select * from workers where work_site='"
								+ possibleValues[counter % 5] + "8'");
			}
			long endTime = System.currentTimeMillis();
			long time_for_execution = endTime - startTime;
			try {
				File file = new File("output_nb_read.txt");
				if (!file.exists()) {
					file.createNewFile();
				}
				FileWriter filewriter = new FileWriter(file.getAbsoluteFile(),
						true);
				BufferedWriter bufferedWriter = new BufferedWriter(filewriter);
				bufferedWriter.write("\nTEST_SIZE = " + testSize
						+ "    time_for_execution = " + time_for_execution);
				System.out.println(testSize + "    time_for_execution = "
						+ time_for_execution);
				// sqlconnection.print();
				runtimes[i] = time_for_execution;
				if (i == iterations - 1) {
					double total = 0;
					for (double value : runtimes) {
						total += value;
					}
					bufferedWriter.write("\nTEST_SIZE = " + testSize
							+ "    average_time_for_execution = " + total
							/ iterations);
					System.out.println(testSize
							+ "    average_time_for_execution = " + total
							/ iterations);
				}
				bufferedWriter.close();

			} catch (Exception e) {
				// }
				// }
			}
		}
		System.out.println("non bitmap read Execution finished");
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {

		}

	}

	public void executeNonBitmapInsert(int testSize) {
		System.out.println("non bitmap insert started");
		sqlconnection.ExecuteQuery("DROP TABLE Workers");
		sqlconnection
				.ExecuteQuery("CREATE TABLE Workers (id int(11) NOT NULL AUTO_INCREMENT, name varchar(20) DEFAULT NULL,work_site varchar(20) DEFAULT NULL,PRIMARY KEY (id))");

		String[] possibleValues = new String[] { "Colombo", "Hambanthota",
				"Kandy", "Jaffna", "Galle" };
		double[] runtimes = new double[iterations];
		for (int i = 0; i < iterations; i++) {
			long startTime = System.currentTimeMillis();
			for (int counter = 0; counter < testSize; counter++) {
				sqlconnection
						.ExecuteQuery("INSERT INTO Workers values (default,'"
								+ "Cham" + counter + "','"
								+ possibleValues[counter % 5] + "')");
			}
			long endTime = System.currentTimeMillis();
			long time_for_execution = endTime - startTime;
			try {
				File file = new File("output_nb_insert.txt");
				if (!file.exists()) {
					file.createNewFile();
				}
				FileWriter filewriter = new FileWriter(file.getAbsoluteFile(),
						true);
				BufferedWriter bufferedWriter = new BufferedWriter(filewriter);
				bufferedWriter.write("\nTEST_SIZE = " + testSize
						+ "    time_for_execution = " + time_for_execution);
				System.out.println(testSize + "    time_for_execution = "
						+ time_for_execution);
				// sqlconnection.print();
				runtimes[i] = time_for_execution;
				if (i == iterations - 1) {
					double total = 0;
					for (double value : runtimes) {
						total += value;
					}
					bufferedWriter.write("\nTEST_SIZE = " + testSize
							+ "    average_time_for_execution = " + total
							/ iterations);
					System.out.println(testSize
							+ "    average_time_for_execution = " + total
							/ iterations);
				}
				bufferedWriter.close();

			} catch (Exception e) {
				// }
				// }
			}
		}
		System.out.println("non bit map insert Execution finished");
	}
}
