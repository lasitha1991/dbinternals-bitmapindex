import java.util.List;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

public class SQLconnection {

	private static Connection connection = null;
	private BitMapIndexGenerator bitmapIndex = null;
	private String user = null;
	private String password = null;
	// private Statement statement = null;
	// private PreparedStatement preparedStatement = null;
	// private ResultSet resultSet = null;
	private final long INSERTION_FAILED_ID = -1;

	public SQLconnection(String user, String password) {
		this.user = user;
		this.password = password;
		this.bitmapIndex = new BitMapIndexGenerator(150, 100);
		this.setConnection();
	}

	private void setConnection() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			connection = DriverManager
					.getConnection("jdbc:mysql://localhost:3306/Internals?"
							+ "user=" + this.user + "&password="
							+ this.password);
		} catch (Exception e) {
			System.out.println("Error setting connection. \n");
			System.out.println("Error while opening connection. \n"
					+ e.toString());
			e.printStackTrace();
			System.out.println("Error end");
		}

	}

	public void ExecuteQuery(String query) {

		try {
			PreparedStatement statement = connection.prepareStatement(query);
			statement.executeUpdate();
//			statement.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}

	}

	public ResultSet ExecuteNonBitmapRead(String query) {

		try {
			PreparedStatement preparedStatement = connection
					.prepareStatement(query);
			// long startTime = System.currentTimeMillis();
			ResultSet r = preparedStatement.executeQuery(query);
			preparedStatement.close();
			// long endTime = System.currentTimeMillis();
			// long time_for_execution = endTime - startTime;
			// System.out.println("prep state: " + time_for_execution);
			return r;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
		return null;
	}

	/**
	 * SELECT queries can be run here.
	 * */
	public ResultSet read(String query, String[] values) {
		try {
			String indexFieldValue = values[0];

			List<Integer> indexList = bitmapIndex.getIdArray(indexFieldValue);
			// bitmapIndex.Print();
			if (indexList.size() != 0) {
				String replacement = "(";
				for (Integer id : indexList) {
					if (!replacement.equals("(")) {
						replacement += " or ";
					}
					replacement += "id=" + id;
					// break;
				}
				replacement+=")";
				query = query.replaceFirst("#", replacement);

				PreparedStatement preparedStatement = connection
						.prepareStatement(query);
				// Statement s=connection.createStatement();

				// preparedStatement.setInt(1, id);
				int local_counter = 0;
				for (String str : values) {
					local_counter++;
					preparedStatement.setString(local_counter, str);
				}
//				System.out.println(query+":"+values[0]);
				// long startTime = System.currentTimeMillis();

				// s.execute(query);

				ResultSet resultSet = preparedStatement.executeQuery();
				preparedStatement.close();				

				// long endTime = System.currentTimeMillis();
				// long time_for_execution = endTime - startTime;
				// System.out.println("prep state: " + time_for_execution);

				// int count=0;
				// while (resultSet.next()) {
				// // System.out.println("The id = " + resultSet.getString("id")
				// // + " name = " + resultSet.getString("name")
				// // + " age_group=" + resultSet.getString("age_group"));
				// count++;
				// }

				return resultSet;
			}
			return null;
		} catch (Exception e) {
			// TODO: handle exception
			System.out
					.print("The read query can\'t be executed. The error is \n "
							+ e.toString());
			e.printStackTrace();
			return null;
		} finally {
			this.close();
		}

	}

	/**
	 * Generic insert method to insert a new record.
	 * */
	public long insert(String query, String[] values) {
		try {
			String indexFieldValue = values[1]; // 0 for worker name
												// 1 for age
			PreparedStatement preparedStatement = connection.prepareStatement(
					query, Statement.RETURN_GENERATED_KEYS);
			int local_counter = 0;
			for (String str : values) {
				local_counter++;
				preparedStatement.setString(local_counter, str);
			}

			int affectted_rows = preparedStatement.executeUpdate();
			if (affectted_rows == 0) {
				return INSERTION_FAILED_ID;
			}

			try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
				if (generatedKeys.next()) {
					bitmapIndex.Insert(indexFieldValue);
					return generatedKeys.getLong(1);
				} else {
					return INSERTION_FAILED_ID;
				}
			}

		} catch (Exception e) {
			System.out
					.println("An error occured in SQLconnection INSERT.\n Inner Exception : "
							+ e.toString());
			e.printStackTrace();
			return INSERTION_FAILED_ID;
			// TODO: handle exception
		} finally {
			this.close();
		}
	}

	/**
	 * Record delete method.
	 * */
	public void delete(String query) {
		try {
			Statement statement = connection.createStatement();
			statement.executeUpdate(query);
		} catch (Exception e) {
			System.out
					.println("Deleting Record not done. See the inner exception below. \n"
							+ e.toString());
		} finally {
			this.close();
		}

	}

	public void update(String query) {
		try {
			Statement statement = connection.createStatement();
			statement.executeUpdate(query);
		} catch (Exception e) {
			System.out
					.println("Updating Record not done. See the inner exception below. \n"
							+ e.toString());
		} finally {
			this.close();
		}

	}

	private void close() {
		// try {
		// // if (resultSet != null) {
		// // resultSet.close();
		// // }
		// // if (statement != null) {
		// // statement.close();
		// // }
		//
		// } catch (Exception e) {
		// System.out.println("Error while closing connection.");
		// }
	}

	public void closeConnection() {
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (Exception e) {
			System.out.print("Error while closing connection. \n"
					+ e.toString());
		}
	}

	public void resetBitmapIndex(int initial_length_of_bit_index,
			int expanding_index_size) {
		this.bitmapIndex = new BitMapIndexGenerator(
				initial_length_of_bit_index, expanding_index_size);
	}

	public void print() {
		bitmapIndex.Print();
	}
}
