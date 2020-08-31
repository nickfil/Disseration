import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

import com.rits.cloning.Cloner;

public class CQA_Multithreaded implements Callable<Map<String, Integer>>  {

   private static Cloner cloner = new Cloner();
   private SQLHandler s;
   private HashMap<String,ArrayList<String>> violating;
   private ArrayList<String> nonViolating;
   private static String tuple;

   public CQA_Multithreaded(SQLHandler s, HashMap<String,ArrayList<String>> violating, ArrayList<String> nonViolating, String tuple) {
      this.s = s;
      this.violating = violating;
      this.nonViolating = nonViolating;
      this.tuple = tuple;
   }

   @Override
   public Map<String, Integer> call(){
      return multithreaded_CQA_helper(s, violating, nonViolating);
   }

   public static Map<String, Integer> multithreaded_CQA_helper(SQLHandler s, HashMap<String,ArrayList<String>> violating, ArrayList<String> nonViolating){
      String tupleToRemove = null;
      String currentKey = null;
      String currentValue = null;
      ArrayList<String> currentQueryResults = new ArrayList<String>();
      Map<String, Integer> results = new HashMap<String, Integer>();

      ArrayList<String> currentTotalInstance = new ArrayList<String>(nonViolating);
      ArrayList<String> removals = new ArrayList<String>();
      HashMap<String, ArrayList<String>> currentInstanceOfViolations = cloner.deepClone(violating);

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

//      currentQueryResults = queryTCC(currentTotalInstance);
//      results = updateResultMap(currentQueryResults, tuple);

      return results;
   }

   public static String pickRandomHashmapValue(HashMap<String, ArrayList<String>> map) {
      String val = "";
      Random rand = new Random();
      int r;
      String key = "";

      Iterator<String> value = map.keySet().iterator();
      key = value.next();

      r = rand.nextInt(map.get(key).size());

      val = key + "@" + map.get(key).get(r);

      return val;
   }

   public static Map<String, Integer> updateResultMap(ArrayList<String> qRes, String tup){
      Map<String, Integer> returnMap = new HashMap<String, Integer>();

      for(String entry : qRes) {
         if(entry.equals(tup))
            returnMap.put(entry, 1);
      }
      return returnMap;
   }

   public static ArrayList<String> queryTCC(ArrayList<String> db){
      ArrayList<String> ret = new ArrayList<String>();

      for(String entry : db) {
         String q1 = entry.split(",")[18];
         //String q2 = entry.split(",")[3];
         if(q1.equals("8")) {// && q2.equals("ROAD CONSTRUCTION/MAINTENANCE")) {
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
            //System.out.println(temp);
            s.executeSQL("INSERT INTO lobbyistsRepair(lobbyist_id, first_name, last_name, address_1, address_2, city, state, zip, country, employer_id, year)  VALUES(" + temp + ")");
         }
     }

      s.query("SELECT * FROM clients,contributions,employers,(select * from lobbyists except select * from lobbyistsrepair) as lobbyists\n" +
               " WHERE contributions.lobbyist_id=lobbyists.lobbyist_id AND employers.employer_id=lobbyists.employer_id AND contribution_date='2017-06-29T00:00:00'");

      s.executeSQL("DROP TABLE lobbyistsRepair");

      return ret;
   }
}
