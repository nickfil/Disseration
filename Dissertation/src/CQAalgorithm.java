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
		
		ArrayList<String> constraintViolating = new ArrayList<String>();
		String tupleToRemove = null;
		Random rand = new Random();
		Map<String, Integer> results = new HashMap<String, Integer>();
		
		double lamda = 0.01;
		double epsilon = 0.01;
		double n = (1/(2*Math.pow(epsilon, 2.0))) * Math.log(2/lamda);
		
		for(int i=0; i<n; i++) {
			Set<String> currentInstance = new HashSet<String>(databaseInstance);
			
			//while the instance is inconsistent
			while(isInconsistent(currentInstance)) {
				constraintViolating = violatingConstraintTuples(currentInstance); //get an ArrayList<String> of all inconsistent tuples
				
				tupleToRemove = constraintViolating.get(rand.nextInt(constraintViolating.size())); //choose one of the violations with prop 1/constraintViolating.length

				currentInstance.remove(tupleToRemove); //update databaseInstance
			}
			
			results = updateResultMap(currentInstance, results);
		
		}
		
		printResults(results, n);
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
		Map<String, Integer> keys = new HashMap<String, Integer>();
		String primaryKey;
		
		for(String tuple : db) {
			primaryKey = tuple.split(",")[0];
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
			primaryKey = tuple.split(",")[0];
			if(keys.get(primaryKey)>1) {
				violations.add(tuple);
			}
		}
		
		return violations;
	}
	
	public static Boolean isInconsistent(Set<String> db) {
		ArrayList<String> keys = new ArrayList<String>();
		
		for(String each : db) {
			if(keys.contains(each.split(",")[0])) {
				return true;
			}
			else {
				keys.add(each.split(",")[0]);
			}
		}
		
		return false;
	}
	
	public static Map<String, Integer> updateResultMap(Set<String> db, Map<String, Integer> res){
		int temp;
		Map<String, Integer> returnMap = new HashMap<String, Integer>(res);
		
		for(String entry : db) {
			temp = 1;
			if(returnMap.containsKey(entry)) {
				temp = returnMap.get(entry)+1;
				returnMap.put(entry, temp);
			}
			else {
				returnMap.put(entry, 1);
			}
		}
		return returnMap;
	}

	public static void printResults(Map<String, Integer> toPrint, double iterations) {
		for(String item : toPrint.keySet()) {
			System.out.println("(" + item + ") - " +  String.format("%.3f", toPrint.get(item)/iterations));
		}
	}
}
