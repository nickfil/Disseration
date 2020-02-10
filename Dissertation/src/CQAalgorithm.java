import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Random; 

import org.json.*;



public class CQAalgorithm {
	private static String username;
	private static String password;
	
	public static void main(String args[]) throws SQLException, JSONException, IOException{	
		
		auth();	
        
		SQLHandler database = new SQLHandler(username, password, "disstester"); //initializing our databse object
		
		ResultSet rs = database.query("SELECT * FROM main;");
		Set<String> databaseInstance = database.getQueryResults(rs);
		//System.out.println(databaseInstance);
		
		double lamda = 0.1;
		double epsilon = 0.1;
		double n = (1/(2*Math.pow(epsilon, 2.0))) * Math.log(2/lamda);
		
		ArrayList<String> constraintViolating = new ArrayList<String>();
		String tupleToRemove = null;
		Random rand = new Random();
		int counter = 0;
		
		for(int i=0; i<n; i++) {
			Set<String> currentInstance = new HashSet<String>(databaseInstance);
			
			//while the instance is inconsistent
			while(isInconsistent(currentInstance)) {
				//get an ArrayList<String> of all inconsistent tuples
				constraintViolating = violatingConstraintTuples(currentInstance);
				
				//choose one of the violations with prop 1/constraintViolating.length
				tupleToRemove = constraintViolating.get(rand.nextInt(constraintViolating.size()));
				//update databaseInstance
				currentInstance.remove(tupleToRemove);
			}
			if(currentInstance.contains("(a,b)")) counter++;
		
		}
		System.out.println(Double.valueOf(counter)/n);
	}

	public static void auth() throws JSONException, IOException {
		 
		BufferedReader reader = new BufferedReader(new FileReader("src/auth.json"));
		JSONObject obj = new JSONObject(reader.readLine());
		reader.close();
	
		JSONArray array = obj.getJSONArray("user");
		username = array.getJSONObject(0).getString("username");
		password = array.getJSONObject(0).getString("password");
        
	}

	public static ArrayList<String> violatingConstraintTuples(Set<String> db){

		ArrayList<String> violations = new ArrayList<String>();
		Map<Character, Integer> keys = new HashMap<Character, Integer>();
		char primaryKey;
		
		for(String tuple : db) {
			primaryKey = tuple.charAt(1);
			if(keys.containsKey(primaryKey)) {
				int count = keys.get(primaryKey);
				count++;
				keys.put(primaryKey, count);
			}
			else {
				keys.put(primaryKey, 1);
			}
		}
		
		for(String tuple : db) {
			primaryKey = tuple.charAt(1);
			if(keys.get(primaryKey)>1) {
				violations.add(tuple);
			}
		}
		
		return violations;
	}
	
	public static Boolean isInconsistent(Set<String> db) {
		ArrayList<Character> keys = new ArrayList<Character>();
		
		for(String each : db) {
			if(keys.contains(each.charAt(1))) {
				return true;
			}
			else {
				keys.add(each.charAt(1));
			}
		}
		
		return false;
	}
}
