import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Random;

import org.json.*;

public class CQAalgorithm {
	private static String username;
	private static String password;
	private static final String q_tcc = "SELECT * FROM locations as l, crashes as c WHERE l.street_name=c.street_name AND l.street_no=c.street_no AND l.street_direction=c.street_direction";
	private static final String q_tcc_1 = "SELECT l.street_name, l.street_no, l.street_direction\n" + 
										  "FROM locations as l, crashes as c WHERE l.street_name=c.street_name AND l.street_no=c.street_no AND l.street_direction=c.street_direction\n" + 
										  "GROUP BY l.street_name, l.street_no, l.street_direction\n" + 
										  "HAVING COUNT(*) > 1";
	//private static final String q_fic = "SELECT * FROM " 
//	private static final String q1_1 = "SELECT * FROM public_experiment_q1_1_10_2_5.lineitem as lineitem, public_experiment_q1_1_10_2_5.partsupp as partsupp WHERE lineitem.l_suppkey = partsupp.ps_suppkey";
//	private static final String q1_2 = "SELECT * FROM public_experiment_q1_1_30_2_5.lineitem as lineitem, public_experiment_q1_1_30_2_5.partsupp as partsupp WHERE lineitem.l_suppkey = partsupp.ps_suppkey";
//	private static final String q1_3 = "SELECT * FROM public_experiment_q1_1_50_2_5.lineitem as lineitem, public_experiment_q1_1_50_2_5.partsupp as partsupp WHERE lineitem.l_suppkey = partsupp.ps_suppkey";
//	private static final String q2_1 = "SELECT * FROM public_experiment_q2_1_10_2_5.lineitem as lineitem, public_experiment_q2_1_10_2_5.partsupp as partsupp, public_experiment_q2_1_10_2_5.supplier as supplier WHERE lineitem.l_suppkey = partsupp.ps_suppkey AND partsupp.ps_suppkey = supplier.s_suppkey";
//	private static final String q2_2 = "SELECT * FROM public_experiment_q2_1_30_2_5.lineitem as lineitem, public_experiment_q2_1_30_2_5.partsupp as partsupp, public_experiment_q2_1_30_2_5.supplier as supplier WHERE lineitem.l_suppkey = partsupp.ps_suppkey AND partsupp.ps_suppkey = supplier.s_suppkey";
//	private static final String q2_3 = "SELECT * FROM public_experiment_q2_1_50_2_5.lineitem as lineitem, public_experiment_q2_1_50_2_5.partsupp as partsupp, public_experiment_q2_1_50_2_5.supplier as supplier WHERE lineitem.l_suppkey = partsupp.ps_suppkey AND partsupp.ps_suppkey = supplier.s_suppkey";
//	private static final String q3_1 = "SELECT * FROM public_experiment_q3_1_10_2_5.lineitem as lineitem, public_experiment_q3_1_10_2_5.orders as orders, public_experiment_q3_1_10_2_5.customer as customer, public_experiment_q3_1_10_2_5.partsupp as partsupp WHERE lineitem.l_orderkey = orders.o_orderkey AND orders.o_custkey = customer.c_custkey AND lineitem.l_suppkey = partsupp.ps_suppkey";
//	private static final String q3_2 = "SELECT * FROM public_experiment_q3_1_30_2_5.lineitem as lineitem, public_experiment_q3_1_30_2_5.orders as orders, public_experiment_q3_1_30_2_5.customer as customer, public_experiment_q3_1_30_2_5.partsupp as partsupp WHERE lineitem.l_orderkey = orders.o_orderkey AND orders.o_custkey = customer.c_custkey AND lineitem.l_suppkey = partsupp.ps_suppkey";
//	private static final String q3_3 = "SELECT * FROM public_experiment_q3_1_50_2_5.lineitem as lineitem, public_experiment_q3_1_50_2_5.orders as orders, public_experiment_q3_1_50_2_5.customer as customer, public_experiment_q3_1_50_2_5.partsupp as partsupp WHERE lineitem.l_orderkey = orders.o_orderkey AND orders.o_custkey = customer.c_custkey AND lineitem.l_suppkey = partsupp.ps_suppkey";

	public static void main(String args[]) throws SQLException, JSONException, IOException{	

		auth();	
		SQLHandler database = new SQLHandler(username, password, "traffic_crashes_chicago"); //initializing our database object
		ResultSet rs = database.query("SELECT * FROM crashes limit 20000"); //("SELECT * FROM main_buildings as mb, facilities as f WHERE mb.license_ = f.license_ limit 150000");
		ArrayList<String> databaseFirstInstance = database.getQueryResultsTCC(rs);
		
		analyzeDB(databaseFirstInstance);

		ArrayList<String> violating = new ArrayList<String>();
		ArrayList<String> nonViolating = new ArrayList<String>();
		Set<String> dups = new HashSet<String>(getDuplicateKeys(databaseFirstInstance));
		
//		for (String a : databaseFirstInstance) {
//			String primaryKey = a.split(",")[0];
//			if(dups.contains(primaryKey)) {
//				violating.add(a);
//			}
//			else {
//				nonViolating.add(a);
//			}
//		}
//		
//		System.out.println("-----------------------------");
//		analyzeDB(violating);
		
		
		double lamda = 0.75;
		double epsilon = 0.1;
		double n = (1/(2*Math.pow(epsilon, 2.0))) * Math.log(2/lamda);
		
		//original_CQA((int)n, databaseFirstInstance);
		//optimised_CQA((int)n, violating, nonViolating);

	}
	
	public static void original_CQA(int n, ArrayList<String> db) {
		ArrayList<String> constraintViolating = new ArrayList<String>();
		String tupleToRemove = null;
		Random rand = new Random();
		ArrayList<String> currentQueryResults = new ArrayList<String>();
		Map<String, Integer> results = new HashMap<String, Integer>();

		long timer = System.currentTimeMillis();
		
		for(int i=1; i<=n; i++) {
			ArrayList<String> currentInstance = new ArrayList<String>(db);
			long iter = System.currentTimeMillis();

			//while the instance is inconsistent
			while(isInconsistent(currentInstance)) {

				constraintViolating = violatingConstraintTuples(currentInstance); //get an ArrayList<String> of all inconsistent tuples	
				
				tupleToRemove = constraintViolating.get(rand.nextInt(constraintViolating.size())); //choose one of the violations with prop 1/constraintViolating.length

				currentInstance.remove(tupleToRemove); //update databaseInstance

			}
			currentQueryResults = queryTCC(currentInstance);
			results = updateResultMap(currentQueryResults, results);

			System.out.println("Iteration " + i + "/" + (int)n + " | Done in " + (System.currentTimeMillis()-iter) + "s");

		}
		printResults(results, n-1);
		System.out.println((System.currentTimeMillis()-timer)/1000);
	}
	
	public static void optimised_CQA(int n, ArrayList<String> violating, ArrayList<String> nonViolating) {
		String tupleToRemove = null;
		Random rand = new Random();
		ArrayList<String> currentQueryResults = new ArrayList<String>();
		Map<String, Integer> results = new HashMap<String, Integer>();
		
		long timer = System.currentTimeMillis();
		
		for(int i=0; i<n; i++) {
			ArrayList<String> currentTotalInstance = new ArrayList<String>(nonViolating);
			ArrayList<String> currentInstanceOfViolations = new ArrayList<String>(violating);
			
			long iter = System.currentTimeMillis();
			
			//while the instance is inconsistent
			while(isInconsistent(currentInstanceOfViolations)) {
				
				tupleToRemove = currentInstanceOfViolations.get(rand.nextInt(currentInstanceOfViolations.size())); //choose one of the violations with prop 1/constraintViolating.length

				currentInstanceOfViolations.remove(tupleToRemove); //update databaseInstance
								
				String tup = nonViolatingConstraintTuple(currentInstanceOfViolations); //add non violating tuple if constraint has been totally removed
				if(!tup.equals("")) { currentTotalInstance.add(tup); }
				
				currentInstanceOfViolations = violatingConstraintTuples(currentInstanceOfViolations); //update constraint violations

			}
			currentQueryResults = queryTCC(currentTotalInstance);
			results = updateResultMap(currentQueryResults, results);

			System.out.println("Iteration " + i + "/" + (int)n + " | Done in " + (System.currentTimeMillis()-iter)/1000 + "s");
		}
		printResults(results, n-1);
		System.out.println("Done in: " + (System.currentTimeMillis()-timer) + "s");
	}

	public static void auth() throws JSONException, IOException {
		 
		BufferedReader reader = new BufferedReader(new FileReader("src/auth.json"));
		JSONObject obj = new JSONObject(reader.readLine());
		reader.close();
	
		JSONArray array = obj.getJSONArray("user");
		username = array.getJSONObject(0).getString("username");
		password = array.getJSONObject(0).getString("password");
        
	}

	public static String nonViolatingConstraintTuple(ArrayList<String> subDB) {
		Set<String> duplicates = new HashSet<String>(getDuplicateKeys(subDB));
		
		for(String each : subDB) {
			if(!duplicates.contains(each.split(",")[0])) {
				return each;
			}
		}
		
		return "";
	}
	
	public static Set<String> getDuplicateKeys(ArrayList<String> db){
		Map<String, Integer> entries = new HashMap<String, Integer>();
		Set<String> duplicatePrimaryKeys = new HashSet<String>();
		String primaryKey;
		
		for(String each : db) {
			primaryKey = each.split(",")[0];
			if(entries.containsKey(primaryKey)) {
				entries.put(primaryKey, entries.get(primaryKey)+1);
			}
			else {
				entries.put(primaryKey, 1);
			}
		}
		
		for(String k : entries.keySet()) {
			if(entries.get(k)>1) {
				duplicatePrimaryKeys.add(k);
			}
		}
		
		return duplicatePrimaryKeys;
	}
	
	//takes a database and returns all rows which violate the primary key constraint
	public static ArrayList<String> violatingConstraintTuples(ArrayList<String> db){

		ArrayList<String> violations = new ArrayList<String>();
		Set<String> duplicateKeys = new HashSet<String>(getDuplicateKeys(db));
		String primaryKey;
		
		for(String tuple : db) {
			primaryKey = tuple.split(",")[0];
			if(duplicateKeys.contains(primaryKey)) {
				violations.add(tuple);
			}
		}
		
		return violations;
			
	}
	
	//checks if the primary key is duplicated inside the given database
	public static Boolean isInconsistent(ArrayList<String> db) {
		Set<String> keys = new HashSet<String>(); //using a set to reduce search time to O(1) - thus runtime of this method is O(n)
		
		for(String each : db) {
			String primaryKey = each.split(",")[0];
			if(keys.contains(primaryKey)) {
				return true;
			}
			else {
				keys.add(primaryKey);
			}
		}
		
		return false;
	}
	
	public static Map<String, Integer> updateResultMap(ArrayList<String> db, Map<String, Integer> res){
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

	public static void analyzeDB(ArrayList<String> db) {
		System.out.println("Total rows: " + db.size());
		
		Set<String> test = new HashSet<String>();
		for(String s : db) {
			String key = s.split(",")[0];
			test.add(key);
		}
		System.out.println("Total unique keys: " + test.size());
		test.clear();
		
		Map<String, Integer> groups = new HashMap<String, Integer>();
		for(String s : db) {
			String key = s.split(",")[0];
			if(groups.containsKey(key)) {
				groups.put(key, groups.get(key)+1);
			}
			else {
				groups.put(key, 1);
			}
		}
		
		int single = 0;
		int multiple = 0;
		int groupedCount = 0;
		for(String item : groups.keySet()) {
			if(groups.get(item)==1) {
				single++;
			}
			else {
				multiple++;
				groupedCount+=groups.get(item);
			}
		}

		System.out.println("Total keys with group=1: " + single);
		System.out.println("Total keys with group>1: " + multiple);
		System.out.println("Average group size: " + Double.valueOf(groupedCount/multiple));
		groups.clear();
		
	}

	public static ArrayList<String> queryTCC(ArrayList<String> db){
		ArrayList<String> ret = new ArrayList<String>();
		
		for(String entry : db) {
			String q = entry.split(",")[10].trim();
			if(q.equals("DRY")) {
				ret.add(entry);
			}
		}
		
		return ret;
	}
	
	public static ArrayList<String> query1(ArrayList<String> db){
		ArrayList<String> ret = new ArrayList<String>();
		
		for(String entry : db) {
			String ps_availqty = entry.split(",")[15].trim();
			String l_tax = entry.split(",")[6].trim();
			if(ps_availqty.equals("674") && l_tax.equals("0.000")) {
				ret.add(entry);
			}
		}
		
		return ret;
	}
	
	public static ArrayList<String> query2(ArrayList<String> db){
		ArrayList<String> ret = new ArrayList<String>();
		// supplier.s_phone = '26-762-352-2798' AND lineitem.l_shipinstruct = 'NONE'
		
		for(String entry : db) {
			String s_phone = entry.split(",")[21].trim();
			String l_shipinstruct = entry.split(",")[12].trim();
			if(s_phone.equals("'26-762-352-2798'") && l_shipinstruct.equals("'NONE'")) {
				ret.add(entry);
			}
		}
		
		return ret;
	}
	
	public static ArrayList<String> query3(ArrayList<String> db){
		ArrayList<String> ret = new ArrayList<String>();
		//  AND partsupp.ps_availqty = 6700 AND orders.o_orderpriority = '1-URGENT'
		
		for(String entry : db) {
			String ps_availqty = entry.split(",")[15].trim();
			String o_orderpriority = entry.split(",")[22].trim();
			if(ps_availqty.equals("6700") && o_orderpriority.equals("'1-URGENT'")) {
				ret.add(entry);
			}
		}
		
		return ret;
	}
}
