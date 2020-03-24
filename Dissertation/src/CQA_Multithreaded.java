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
   ArrayList<String> nonViolating;

   public CQA_Multithreaded(SQLHandler s, HashMap<String,ArrayList<String>> violating, ArrayList<String> nonViolating) {
      this.s = s;
      this.violating = violating;
      this.nonViolating = nonViolating;
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

      currentQueryResults = queryTCC(currentTotalInstance);
      results = updateResultMap(currentQueryResults, results);

      return results;
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
}
