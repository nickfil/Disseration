import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class RandomQueryGenerator {

	private static String username;
	private static String password;
	private static ArrayList<String> combs = new ArrayList<String>();
		
	public static void main(String[] args) throws JSONException, IOException, SQLException {
		String[] tablesArray = {"crashes", "locations"};
		ArrayList<String> tables = new ArrayList<String>(Arrays.asList(tablesArray));
		String[] keysForeignKeysArray = {"crashes.crash_date,crashes.street_name,crashes.street_no,crashes.street_direction", "locations.street_name,locations.street_no,locations.street_direction"};
		ArrayList<String> keysForeignKeys = new ArrayList<String>(Arrays.asList(keysForeignKeysArray));
		int number = 2; //number of joins
		int filters = 2; //number of filters
		int numberOfQueries = 5; //number of queries

		auth();
		SQLHandler database = new SQLHandler(username, password, "traffic_crashes_chicago"); //initializing our database object
		
		/***********************NUMBER OF QUERIES*****************/
		
		for(int a=0; a<numberOfQueries; a++) {	
			generateQuery(database, tables, keysForeignKeys, number, filters);	
		}
	}
	
	public static void generateQuery(SQLHandler database, ArrayList<String> tables, ArrayList<String> keysForeignKeys, int number, int filters) throws SQLException {
		/* Choosing the number of tables */
		Random rand = new Random();
		int indexToRemove;
		int iterations = tables.size()-number;
		
		for(int i=0; i<iterations; i++) {
			indexToRemove = rand.nextInt(tables.size());
			tables.remove(indexToRemove);
			keysForeignKeys.remove(indexToRemove);
		}
		/**********************************/
		
		String where = getWhere(keysForeignKeys);
		String initialQuery = "SELECT * FROM " + String.join(",", tables) + where + " ORDER BY random() limit 1";
		ResultSet rs = database.query(initialQuery);
		ResultSetMetaData meta = rs.getMetaData();
		
		ArrayList<String> columnNames = new ArrayList<String>(getColumnNames(meta));
		
		iterations = columnNames.size() - filters;
		/***Choosing random filters***/
		for(int i=0; i<iterations; i++) {
			indexToRemove = rand.nextInt(columnNames.size());
			columnNames.remove(indexToRemove);
		}
		/***************************/
		
		String finalFilters = getFilters(columnNames, rs);
		
		
		String finalQuery = "SELECT * FROM " + String.join(",", tables) + where + finalFilters;
		
		System.out.println(finalQuery);
	}
	
	public static String getFilters(ArrayList<String> columns, ResultSet dbQuery) throws SQLException {
		String ret = "AND ";
		
		if(dbQuery.next()) {
			for(int i=0; i<columns.size(); i++) {
				ret += columns.get(i) + "=" + dbQuery.getString(columns.get(i));
				
				if(columns.size()>1 && i<columns.size()-1) {
					ret += " AND ";
				}
			}
		}
		
		return ret;
	}
	
	public static ArrayList<String> getColumnNames(ResultSetMetaData m) throws SQLException {
		ArrayList<String> ret = new ArrayList<String>();
		
		for(int j=1; j<=m.getColumnCount(); j++) {
			ret.add(m.getColumnName(j));
		}
		
		return ret;
	}
	
	public static String getWhere(ArrayList<String> ks) {
		combs.clear();
		String where = " ";
		ArrayList<String> fullList = new ArrayList<String>();
		ArrayList<String> combinations = new ArrayList<String>();
		
		for (String key : ks) {
			fullList.addAll(Arrays.asList(key.split(",")));
		}
		String[] a = {};
		a = fullList.toArray(a);
		
		recur(a, "", 0, fullList.size(), 2);
		
		for(String com : combs) {
			String left = com.split("=")[1];
			String right = com.split("=")[2];
			//System.out.println(left.split("\\.")[1] + "===" + right.split("\\.")[1]);
			if(left.split("\\.")[1].equals(right.split("\\.")[1])) {
				combinations.add(" "+left+"="+right+" ");
			}
		}
		
		where = " WHERE" + String.join("AND", combinations);
		
		return where;
	}
		
	public static void recur(String[] A, String out, int i, int n, int k) {
 
        // base case: combination size is k
        if (k == 0) {
            //System.out.println(out);
            combs.add(out);
        }
 
        // start from next index till last index
        for (int j = i; j < n; j++)
        {
            // add current element A[j] to solution & recur for next index
            // (j+1) with one less element (k-1)
            recur(A, out + "=" + (A[j]) , j + 1, n, k - 1);
 
            // uncomment below code to handle duplicates
             while (j < n - 1 && A[j] == A[j + 1]) {
                 j++;
            }
        }
		//return out;
        
    }
	
	public static void auth() throws JSONException, IOException {
		 
		BufferedReader reader = new BufferedReader(new FileReader("src/auth.json"));
		JSONObject obj = new JSONObject(reader.readLine());
		reader.close();
	
		JSONArray array = obj.getJSONArray("user");
		username = array.getJSONObject(0).getString("username");
		password = array.getJSONObject(0).getString("password");
        
	}
	
}
