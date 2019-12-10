import java.sql.*;
import java.util.ArrayList;

import javafx.util.Pair;

public class SQLHandler {
	
	private static String username;
	private static String password;
	private static String dbName;
	private static Connection conn = null;
	private static Statement sql;
	
	public SQLHandler(String username, String password, String dbName) throws SQLException {
		this.username = username;
		this.password = password;
		this. dbName = dbName;
		
		
		setupConnection();
		initializeDB();
	}
	
	private static void setupConnection() {
		System.out.println("——– PostgreSQL "+ "JDBC Connection Testing ————");

		try {

			Class.forName("org.postgresql.Driver");

		} catch (ClassNotFoundException e) {

			System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
			e.printStackTrace();
			return;

		}

		System.out.println("PostgreSQL JDBC Driver Registered!");
		
		try {

			String url = "jdbc:postgresql://localhost/" + dbName + "?user=" + username + "&password=" + password + "&ssl=false";
			conn = DriverManager.getConnection(url);
			System.out.println("Connection Succeeded");

		} catch (SQLException e) {

			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;

		}
		
	}
	
	private static void initializeDB() throws SQLException {
		conn.createStatement().executeUpdate("INSERT INTO main VALUES ('a', 'b')");
		conn.createStatement().executeUpdate("INSERT INTO main VALUES ('a', 'c')");
		conn.createStatement().executeUpdate("INSERT INTO main VALUES ('a', 'd')");
		conn.createStatement().executeUpdate("INSERT INTO main VALUES ('b', 'a')");
		conn.createStatement().executeUpdate("INSERT INTO main VALUES ('b', 'd')");
		conn.createStatement().executeUpdate("INSERT INTO main VALUES ('c', 'a')");
	}
	
	public void clearDB() throws SQLException {
		conn.createStatement().executeUpdate("DELETE FROM main");
	}
	
	public ResultSet query(String q) throws SQLException {
		sql = conn.createStatement();
		ResultSet queryResult = null;
		
		if (conn != null) {
			queryResult = sql.executeQuery(q);
		} else {
			System.out.println("Failed to make connection!");
		}
		
		return queryResult;
		
	}
	
	public void printQueryResults(ResultSet res) throws SQLException {
		
		while (res.next()) {
            String key = res.getString("keys");
            String values = res.getString("values");
            System.out.println(key + " " + values);
		}
	}
	
	public ArrayList<Pair<String, String>> getQueryResults(ResultSet res) throws SQLException {
		
		ArrayList<Pair<String, String>> queryResults = new ArrayList<Pair<String, String>>();
		
		while (res.next()) {
            String key = res.getString("keys");
            String values = res.getString("values");
            
            Pair<String, String> p = new Pair<String, String>(key, values);
            queryResults.add(p);
		}
		
		return queryResults;
	}

}
