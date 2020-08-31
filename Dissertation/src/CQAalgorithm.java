import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.rits.cloning.Cloner;

public class CQAalgorithm {
	private static String username;
	private static String password;
	private static final String q_tcc = "SELECT * FROM locations as l, crashes as c WHERE l.street_name=c.street_name AND l.street_no=c.street_no AND l.street_direction=c.street_direction";
	private static final String q_lob = "SELECT * FROM lobbyists";
	static Cloner cloner=new Cloner();
	private static final String q1_1 = "SELECT * FROM public_experiment_q1_1_10_2_5.lineitem as lineitem, public_experiment_q1_1_10_2_5.partsupp as partsupp WHERE lineitem.l_suppkey = partsupp.ps_suppkey";

	public static void main(String args[]) throws SQLException, JSONException, IOException, InterruptedException, ExecutionException{

		auth();
		SQLHandler database = new SQLHandler(username, password, "out1_1"); //initializing our database object
		ResultSet rs = database.query(q1_1); 
		System.out.println("----------------------");
		ArrayList<String> databaseFirstInstance = database.getQueryResults1(rs);
		System.out.println("----------------------");

		analyzeDB(databaseFirstInstance);

		HashMap<String, ArrayList<String>> violatingMap = new HashMap<String, ArrayList<String>>();
		ArrayList<String> violating = new ArrayList<String>();
		ArrayList<String> nonViolating = new ArrayList<String>();
		Map<String, Integer> multipleEntries = new HashMap<String, Integer>(getDuplicateKeys(databaseFirstInstance));


		for (String a : databaseFirstInstance) {
			String primaryKey = a.split(",")[0];
			String value = a.substring(primaryKey.length()+1);
			if(multipleEntries.containsKey(primaryKey)) {

				if(violatingMap.containsKey(primaryKey))
				  violatingMap.get(primaryKey).add(value);
				else
				   violatingMap.put(primaryKey, new ArrayList<String>(Arrays.asList(value)));
			}
		}

		System.out.println("----------------------");
		//analyzeDB(violating);


		double lamda = 0.75;
		double epsilon = 0.1;
		double n = (1/(2*Math.pow(epsilon, 2.0))) * Math.log(2/lamda);

		//original_CQA((int)n, databaseFirstInstance);
		//optimised_CQA(database, (int)n, violating, nonViolating, multipleEntries);
		//more_optimised_CQA(database, (int)n, violatingMap, nonViolating);
		multithreaded_CQA(database, (int)n, violatingMap, nonViolating);
	}

	public static void multithreaded_CQA(SQLHandler s, int n, HashMap<String,ArrayList<String>> violating, ArrayList<String> nonViolating) throws SQLException, InterruptedException, ExecutionException {

	   //System.out.println(Runtime.getRuntime().availableProcessors());
	   String tupleOfInterest = "BYRON ST/4800/W/2019-04-04T10:34:00,30,STOP SIGN/FLASHER,FUNCTIONING PROPERLY,RAIN,DAYLIGHT,ANGLE,DIVIDED - W/MEDIAN (NOT RAISED),4,STRAIGHT AND LEVEL,WET,NO DEFECTS,-87.747394493,FAILING TO REDUCE SPEED TO AVOID CRASH,FAILING TO YIELD RIGHT-OF-WAY,0,0,0,1,2,0,41.951680612,null";
	   int occurences = 0;
	   ExecutorService service = Executors.newFixedThreadPool(6);
	   Future<Map<String, Integer>> res = null;
	   ArrayList<Future<Map<String, Integer>>> aggregate = new ArrayList<Future<Map<String, Integer>>>();

	   long timer = System.currentTimeMillis();

	   for(int i=0; i<n; i++) {
	      res = service.submit(new CQA_Multithreaded(s, violating, nonViolating, tupleOfInterest));
	      aggregate.add(res);
	   }

	   while(!res.isDone()) { /*do nothing, just wait*/ }

	   for(Future<Map<String, Integer>> map : aggregate) {
	      for(String k : map.get().keySet()) {
	         if(k.equals(tupleOfInterest))
	            occurences++;
	      }
	   }

	   System.out.println((System.currentTimeMillis()-timer));

	   service.shutdown();
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

				tupleToRemove = constraintViolating.get(rand.nextInt(constraintViolating.size())); //choose one of the violations with prop 1/constraintViolating.length

				currentInstance.remove(tupleToRemove); //update databaseInstance

			}
			currentQueryResults = queryTCC(currentInstance);
			results = updateResultMap(currentQueryResults, results);

			System.out.println("Iteration " + i + "/" + n + " | Done in " + (System.currentTimeMillis()-iter) + "s");

		}
		printResults(results, n-1);
		System.out.println((System.currentTimeMillis()-timer)/1000);
	}

	public static void optimised_CQA(SQLHandler s, int n, ArrayList<String> violating, ArrayList<String> nonViolating, Map<String, Integer> multipleEntries) throws SQLException {
		String tupleToRemove = null;
		Random rand = new Random();
		String tup = "";
		ArrayList<String> currentQueryResults = new ArrayList<String>();
		Map<String, Integer> results = new HashMap<String, Integer>();

		long timer = System.currentTimeMillis();

		for(int i=0; i<n; i++) {
			ArrayList<String> currentTotalInstance = new ArrayList<String>(nonViolating);
			ArrayList<String> currentInstanceOfViolations = new ArrayList<String>(violating);
			Map<String, Integer> multipleEntriesTemp = new HashMap<String, Integer>(multipleEntries);


			long iter = System.currentTimeMillis();

			//while the instance is inconsistent
			while(isInconsistent(currentInstanceOfViolations)) {
				tupleToRemove = currentInstanceOfViolations.get(rand.nextInt(currentInstanceOfViolations.size())); //choose one of the violations with prop 1/constraintViolating.length

				currentInstanceOfViolations.remove(tupleToRemove); //update violating databaseInstance

				if(multipleEntriesTemp.get(tupleToRemove.split(",")[0])==2) {
					tup = nonViolatingConstraintTuple(currentInstanceOfViolations, tupleToRemove); //getting tuple which is now consistent
					currentTotalInstance.add(tup);
					currentInstanceOfViolations.remove(tup);
					multipleEntriesTemp.remove(tupleToRemove.split(",")[0]); //add non violating tuple if constraint has been totally removed
				}
				else {
					multipleEntriesTemp.put(tupleToRemove.split(",")[0], multipleEntriesTemp.get(tupleToRemove.split(",")[0])-1);
				}

			}

			currentQueryResults = queryLob(s, currentTotalInstance);
			results = updateResultMap(currentQueryResults, results);

			System.out.println("Iteration " + i + "/" + n + " | Done in " + (System.currentTimeMillis()-iter)/1000 + "s");
		}
		//printResults(results, n-1);
		System.out.println("Done in: " + (System.currentTimeMillis()-timer)/1000 + "s");
	}

	public static void more_optimised_CQA(SQLHandler s, int n, HashMap<String,ArrayList<String>> violating, ArrayList<String> nonViolating) throws SQLException {
      String tupleToRemove = null;
      String currentKey = null;
      String currentValue = null;
      ArrayList<String> currentQueryResults = new ArrayList<String>();
      Map<String, Integer> results = new HashMap<String, Integer>();

      long timer = System.currentTimeMillis();

      for(int i=0; i<n; i++) {
         ArrayList<String> currentTotalInstance = new ArrayList<String>(nonViolating);
         ArrayList<String> removals = new ArrayList<String>();
         HashMap<String, ArrayList<String>> currentInstanceOfViolations = cloner.deepClone(violating);

         long iter = System.currentTimeMillis();

         //while the instance is inconsistent
         while(!currentInstanceOfViolations.isEmpty()) {

            tupleToRemove = pickRandomHashmapValue(currentInstanceOfViolations); //remove a random tuple
            currentKey = tupleToRemove.split("@")[0];
            currentValue = tupleToRemove.split("@")[1];

            currentInstanceOfViolations.get(currentKey).remove(currentValue); //remove tuple from violation set
            removals.add(currentKey + "," + currentValue); //add tuple to the ones to be removed

            if(currentInstanceOfViolations.get(currentKey).size()==1) { //if the violation has been removed, we need to also remove it from the set of violations
               currentTotalInstance.add(currentKey + "," + currentInstanceOfViolations.get(currentKey).get(0));
               currentInstanceOfViolations.remove(currentKey);
            }

         }

         currentQueryResults = queryTCC(currentTotalInstance);
         results = updateResultMap(currentQueryResults, results);

         System.out.println("Iteration " + (i+1) + "/" + n + " | Done in " + (System.currentTimeMillis()-iter) + "ms");
      }
      System.out.println("Done in: " + (System.currentTimeMillis()-timer)/1000 + "s");
   }

	public static String pickRandomHashmapValue(HashMap<String, ArrayList<String>> map) {
	   String val = "";
      Random rand = new Random();
      int r;
      int index = 0;
      String key = "";

      Iterator<String> value = map.keySet().iterator();
      key = value.next();

      r = rand.nextInt(map.get(key).size());

	   val = key + "@" + map.get(key).get(r);

	   return val;
	}

	public static void auth() throws JSONException, IOException {

		BufferedReader reader = new BufferedReader(new FileReader("src/auth.json"));
		JSONObject obj = new JSONObject(reader.readLine());
		reader.close();

		JSONArray array = obj.getJSONArray("user");
		username = array.getJSONObject(0).getString("username");
		password = array.getJSONObject(0).getString("password");

	}

	public static String nonViolatingConstraintTuple(ArrayList<String> subDB, String removedTuple) {
		String key = removedTuple.split(",")[0];

		for(String each : subDB) {
			if(each.split(",")[0].equals(key)) {
				return each;
			}
		}

		return "";
	}

	public static Map<String, Integer> getDuplicateKeys(ArrayList<String> db){
      Map<String, Integer> entries = new HashMap<String, Integer>();
		Map<String, Integer> duplicatePrimaryKeysOnly = new HashMap<String, Integer>();
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
				duplicatePrimaryKeysOnly.put(k, entries.get(k));
			}
		}

		return duplicatePrimaryKeysOnly;

	}

	//takes a database and returns all rows which violate the primary key constraint
	public static ArrayList<String> violatingConstraintTuples(ArrayList<String> db, Map<String, Integer> duplicateKeys){

		ArrayList<String> violations = new ArrayList<String>();
		String primaryKey;

		for(String tuple : db) {
			primaryKey = tuple.split(",")[0];
			if(duplicateKeys.containsKey(primaryKey)) {
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
			String q1 = entry.split(",")[2];
			//String q2 = entry.split(",")[3];
			if(q1.equals("STOP SIGN/FLASHER")) {// && q2.equals("ROAD CONSTRUCTION/MAINTENANCE")) {
				ret.add(entry);
			}
		}

		return ret;
	}

	public static ArrayList<String> queryLob(SQLHandler s, ArrayList<String> db) throws SQLException{
      ArrayList<String> ret = new ArrayList<String>();
      s.executeSQL("CREATE TABLE lobbyistsRepair AS TABLE lobbyists WITH NO DATA");
      String temp = "";

      for(String row : db) {
         temp="";
         if(row.split(",").length==11 && !row.contains("'")) {
            for(String each : row.split(",")) {
               temp+= "'" + each + "',";
            }
            temp = temp.substring(0, temp.length()-1);
            
            s.executeSQL("INSERT INTO lobbyistsRepair(lobbyist_id, first_name, last_name, address_1, address_2, city, state, zip, country, employer_id, year)  VALUES(" + temp + ")");
         }
     }

      s.query("SELECT * FROM clients,contributions,employers,(select * from lobbyists except select * from lobbyistsrepair) as lobbyists\n" +
               " WHERE contributions.lobbyist_id=lobbyists.lobbyist_id AND employers.employer_id=lobbyists.employer_id AND contribution_date='2017-06-29T00:00:00'");

      s.executeSQL("DROP TABLE lobbyistsRepair");

      return ret;
   }

}
