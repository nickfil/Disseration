import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import org.json.*;



public class CQAalgorithm {
	private static String username;
	private static String password;
	
	public static void main(String args[]) throws SQLException, JSONException, IOException{	
		
		auth();	
        
		SQLHandler database = new SQLHandler(username, password, "disstester"); //initializing our databse object
		
		ResultSet rs = database.query("SELECT * FROM main;");
		System.out.println(database.getQueryResults(rs));
		database.clearDB();
		
		
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
