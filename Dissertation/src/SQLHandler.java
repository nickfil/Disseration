import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

//sudo pkill -u postgres
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
		//initializeDB();
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
	
	public Set<String> getQueryResultsTest(ResultSet res) throws SQLException {
		
		Set<String> queryResults = new HashSet<String>();
		
		while (res.next()) {
            String key = res.getString("keys");
            String values = res.getString("values");
            
            queryResults.add(key+","+values);
		}
		
		return queryResults;
	}

	public ArrayList<String> getQueryResults1(ResultSet res) throws SQLException {
			
			ArrayList<String> queryResults = new ArrayList<String>();
			String[] resultList = new String[17];
			String[] key = new String[4];
 			
			while (res.next()) {
	            key[0] = res.getString("l_orderkey");
	            key[1] = res.getString("ps_partkey");
	            key[2] = res.getString("ps_suppkey");
	            key[3] = res.getString("l_linenumber");
	            
	            resultList[0] = res.getString("l_partkey");
	            resultList[1] = res.getString("l_suppkey");
	            resultList[2] = res.getString("l_quantity");
	            resultList[3] = res.getString("l_extendedprice");
	            resultList[4] = res.getString("l_discount");
	            resultList[5] = res.getString("l_tax");
	            resultList[6] = res.getString("l_returnflag");
	            resultList[7] = res.getString("l_linestatus");
	            resultList[8] = res.getString("l_shipdate");
	            resultList[9] = res.getString("l_commitdate");
	            resultList[10] = res.getString("l_receiptdate");
	            resultList[11] = res.getString("l_shipinstruct");
	            resultList[12] = res.getString("l_shipmode");
	            resultList[13] = res.getString("l_comment");
	            resultList[14] = res.getString("ps_availqty");
	            resultList[15] = res.getString("ps_supplycost");
	            resultList[16] = res.getString("ps_comment");
	            
	            queryResults.add(String.join("/", key) + "," + String.join(",", resultList));
			}
			
			return queryResults;
		}

	public ArrayList<String> getQueryResults2(ResultSet res) throws SQLException {
		
		ArrayList<String> queryResults = new ArrayList<String>();
		String[] resultList = new String[23];
		String[] key = new String[5];
			
		while (res.next()) {
            key[0] = res.getString("l_orderkey");
            key[1] = res.getString("ps_partkey");
            key[2] = res.getString("ps_suppkey");
            key[3] = res.getString("l_linenumber");
            key[4] = res.getString("s_suppkey");
            
            resultList[0] = res.getString("l_partkey");
            resultList[1] = res.getString("l_suppkey");
            resultList[2] = res.getString("l_quantity");
            resultList[3] = res.getString("l_extendedprice");
            resultList[4] = res.getString("l_discount");
            resultList[5] = res.getString("l_tax");
            resultList[6] = res.getString("l_returnflag");
            resultList[7] = res.getString("l_linestatus");
            resultList[8] = res.getString("l_shipdate");
            resultList[9] = res.getString("l_commitdate");
            resultList[10] = res.getString("l_receiptdate");
            resultList[11] = res.getString("l_shipinstruct");
            resultList[12] = res.getString("l_shipmode");
            resultList[13] = res.getString("l_comment");
            resultList[14] = res.getString("ps_availqty");
            resultList[15] = res.getString("ps_supplycost");
            resultList[16] = res.getString("ps_comment"); 
            resultList[17] = res.getString("s_name");
            resultList[18] = res.getString("s_address");
            resultList[19] = res.getString("s_nationkey");
            resultList[20] = res.getString("s_phone");
            resultList[21] = res.getString("s_acctbal");
            resultList[22] = res.getString("s_comment");
            
            queryResults.add(String.join("/", key) + "," + String.join(",", resultList));
		}
		
		return queryResults;
	}
	
	public ArrayList<String> getQueryResults3(ResultSet res) throws SQLException {
		
		ArrayList<String> queryResults = new ArrayList<String>();
		String[] resultList = new String[31];
		String[] key = new String[6];
			
		while (res.next()) {
            key[0] = res.getString("l_orderkey");
            key[1] = res.getString("ps_partkey");
            key[2] = res.getString("ps_suppkey");
            key[3] = res.getString("l_linenumber");
            key[4] = res.getString("o_orderkey");
            key[5] = res.getString("c_custkey");
            
            resultList[0] = res.getString("l_partkey");
            resultList[1] = res.getString("l_suppkey");
            resultList[2] = res.getString("l_quantity");
            resultList[3] = res.getString("l_extendedprice");
            resultList[4] = res.getString("l_discount");
            resultList[5] = res.getString("l_tax");
            resultList[6] = res.getString("l_returnflag");
            resultList[7] = res.getString("l_linestatus");
            resultList[8] = res.getString("l_shipdate");
            resultList[9] = res.getString("l_commitdate");
            resultList[10] = res.getString("l_receiptdate");
            resultList[11] = res.getString("l_shipinstruct");
            resultList[12] = res.getString("l_shipmode");
            resultList[13] = res.getString("l_comment");
            resultList[14] = res.getString("ps_availqty");
            resultList[15] = res.getString("ps_supplycost");
            resultList[16] = res.getString("ps_comment"); 
            resultList[17] = res.getString("o_custkey");
            resultList[18] = res.getString("o_orderstatus");
            resultList[19] = res.getString("o_totalprice");
            resultList[20] = res.getString("o_orderdate");
            resultList[21] = res.getString("o_orderpriority");
            resultList[22] = res.getString("o_clerk");
            resultList[23] = res.getString("o_shippriority");
            resultList[23] = res.getString("o_comment");      
            resultList[24] = res.getString("c_name");
            resultList[25] = res.getString("c_address");
            resultList[26] = res.getString("c_nationkey");
            resultList[27] = res.getString("c_phone");
            resultList[28] = res.getString("c_acctbal");
            resultList[29] = res.getString("c_mktsegment");
            resultList[30] = res.getString("c_comment"); 
            
            queryResults.add(String.join("/", key) + "," + String.join(",", resultList));
		}
		
		return queryResults;
	}

	public ArrayList<String> getQueryResultsTCC(ResultSet res) throws SQLException {
		
		ArrayList<String> queryResults = new ArrayList<String>();
		String[] resultList = new String[22];
		String[] key = new String[4];

		while (res.next()) {
            key[0] = res.getString("street_name");
            key[1] = res.getString("street_no");
            key[2] = res.getString("street_direction");
            key[3] = res.getString("crash_date");
            resultList[0] = res.getString("posted_speed_limit");
            resultList[1] = res.getString("traffic_control_device");
            resultList[2] = res.getString("device_condition");
            resultList[3] = res.getString("weather_condition");
            resultList[4] = res.getString("lighting_condition");
            resultList[5] = res.getString("first_crash_type");
            resultList[6] = res.getString("trafficway_type");
            resultList[7] = res.getString("lane_cnt");
            resultList[8] = res.getString("alignment");
            resultList[9] = res.getString("roadway_surface_cond");
            resultList[10] = res.getString("road_defect");
            resultList[11] = res.getString("crash_type");
            resultList[12] = res.getString("prim_contributory_cause");
            resultList[13] = res.getString("sec_contributory_cause");
            resultList[14] = res.getString("injuries_fatal");
            resultList[15] = res.getString("injuries_incapacitating");
            resultList[16] = res.getString("injuries_non_incapacitating");
            resultList[17] = res.getString("injuries_reported_not_evident");
            resultList[18] = res.getString("injuries_no_indication");
            resultList[19] = res.getString("injuries_unknown");
            resultList[20] = res.getString("latitude");
            resultList[11] = res.getString("longitude");

  
            queryResults.add(String.join("/", key) + "," + String.join(",", resultList));
		}
		return queryResults;
	}

	public ArrayList<String> getQueryResultsFIC(ResultSet res) throws SQLException {
		
		ArrayList<String> queryResults = new ArrayList<String>();
		String[] resultList = new String[6];
		String[] key = new String[2];
		
		while (res.next()) {
            key[0] = res.getString("license_");
            key[1] = res.getString("aka_name");
            //key[2] = res.getString("inspection_id");
            //from main_buildings table 
            resultList[0] = res.getString("dba_name");
            resultList[1] = res.getString("address");
            resultList[2] = res.getString("city");
            resultList[3] = res.getString("state");
            resultList[4] = res.getString("zip");
            //from facilities table
            resultList[5] = res.getString("facility_type");
//            //from inspections table
//            resultList[1] = res.getString("inspection_date");
//            resultList[2] = res.getString("inspection_type");
//            resultList[3] = res.getString("risk");
//            resultList[4] = res.getString("results");
            
  
            queryResults.add(String.join("/", key) + "," + String.join(",", resultList));
		}
		return queryResults;
	}

	public ArrayList<String> getQueryResultsLobbyists(ResultSet res) throws SQLException {
		
		ArrayList<String> queryResults = new ArrayList<String>();
		String[] resultList = new String[26];
		String[] key = new String[3];
		
		while (res.next()) {
			key[0] = res.getString("lobbyist_id");
			key[1] = res.getString("lobbying_activity_id");
//			key[1] = res.getString("gift_id");
//			key[1] = res.getString("employer_id");
//			key[1] = res.getString("contribution_id");
//			key[1] = res.getString("compensation_id"); 
            key[2] = res.getString("client_id");

//            //from clients table
            resultList[0] = res.getString("name");
            resultList[1] = res.getString("address_1");
            resultList[2] = res.getString("address_2");
            resultList[3] = res.getString("city");
            resultList[4] = res.getString("state");
            resultList[5] = res.getString("zip");
            resultList[6] = res.getString("country");
            resultList[7] = res.getString("created_date");
            resultList[8] = res.getString("active");
			
//			//from compensations table
//			resultList[0] = res.getString("period_start");
//			resultList[1] = res.getString("period_end");
//			resultList[2] = res.getString("compensation_amount");
//			resultList[13] = res.getString("client_id");
			
//			//from contributions table
//			resultList[0] = res.getString("contribution_date");
//			resultList[1] = res.getString("recipient");
//			resultList[2] = res.getString("amount");
			
			//from employer table
//			resultList[0] = res.getString("name");
//			resultList[1] = res.getString("address_1");
//			resultList[2] = res.getString("address_2");
//			resultList[3] = res.getString("city");
//			resultList[4] = res.getString("state");
//			resultList[5] = res.getString("zip");
//			resultList[6] = res.getString("country");
//			resultList[7] = res.getString("active");
			
			//from gifts table
//			resultList[0] = res.getString("period_start");
//			resultList[1] = res.getString("period_end");
//			resultList[2] = res.getString("gift");
//			resultList[3] = res.getString("recipient_first_name");
//			resultList[4] = res.getString("recipient_last_name");
//			resultList[5] = res.getString("recipient_title");
//			resultList[6] = res.getString("value");
//			resultList[7] = res.getString("department");
//			resultList[8] = res.getString("lobbyist_id");
			
//			//from lobbying activities table
			resultList[9] = res.getString("period_start");
			resultList[10] = res.getString("period_end");
			resultList[11] = res.getString("action");
			resultList[12] = res.getString("action_sought");
			resultList[13] = res.getString("department");
			resultList[14] = res.getString("client_id");
			resultList[15] = res.getString("lobbyist_id");
			
			//from lobbyists table
			resultList[16] = res.getString("first_name");
			resultList[17] = res.getString("last_name");
			resultList[18] = res.getString("address_1");
			resultList[19] = res.getString("address_2");
			resultList[20] = res.getString("city");
			resultList[21] = res.getString("state");
			resultList[22] = res.getString("zip");
			resultList[23] = res.getString("country");
			resultList[24] = res.getString("employer_id");
			resultList[25] = res.getString("year");

			
            queryResults.add(String.join("/", key) + "," + String.join(",", resultList));
		}
		return queryResults;
	}

}
