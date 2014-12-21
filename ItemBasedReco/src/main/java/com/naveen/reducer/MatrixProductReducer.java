package com.naveen.reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MatrixProductReducer
  extends Reducer<Text, Text, Text, Text>
{
  
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
  {

		
		
		System.out.println("In Reducer");
		HashMap<String, Integer> itemProduct = new HashMap<String, Integer>();
		
		String [] itemDetail = new String[2];
		
 		for(Text value:values){
 			
 			itemDetail = value.toString().split(":");
 			
 			//System.out.println("item detail array:" + Arrays.toString(itemDetail));
 			
 			if (itemProduct.get(itemDetail[0]) ==null){
 				//System.out.println("Record is not present:" + itemDetail[0]);
 				//System.out.println("ItemProduct Size in the else clause:" + itemProduct.size());
 				//if(itemProduct.size() < 11){
 				  itemProduct.put(itemDetail[0], Integer.parseInt(itemDetail[1]));
 				//}
 			}
 			else{
 				//System.out.println("Record is present:" + itemDetail[0]);
 				//System.out.println("ItemProduct Size in the if clause:" + itemProduct.size());
 				//if(itemProduct.size() < 11){
 				  itemProduct.put(itemDetail[0], (Integer.parseInt(itemDetail[1]) + (Integer) itemProduct.get(itemDetail[0])));
 				/*}
 				else
 					continue;*/
 			}
		}
 		
 		//Sorting HashMap by values for fetching the top 5 items from the itemProduct hashmap
 		
 		TreeMap<String, Integer> sortedMap = sortByValues(itemProduct);
 		
 		StringBuilder str = new StringBuilder();
 		
 		int i = 0;
 		for (Map.Entry<String, Integer> entry : sortedMap.entrySet()){
 			  
 			  //Logic to print only the first 5 items from the Treemap
 			  if(i >5){
 				  break;
 			  }
 			  str.append(entry.getKey().toString());
 		      str.append("\t");
 		      str.append(entry.getValue().toString());
 		      
 		      //Writing all the output values to the Context object
 		      try {
				context.write(key,new Text(str.toString()));
			  } catch (IOException | InterruptedException e) {
				e.printStackTrace();
			  }
 		      
 		      //Resetting the below string to empty the StringBuilder variable
 		      str.setLength(0);
 		      str.trimToSize();
 		      i++;
 		}
 				
	}

  
/*
 * Method sorts the HashMap by values and returns a sorted map
 */
private static TreeMap<String, Integer> sortByValues(HashMap<String, Integer> map) {
	
	ValueComparator vc = new ValueComparator(map);
	
	//Declaring a TreeMap for storing the sorted map
	TreeMap<String, Integer> sortedMap = new TreeMap<String, Integer>(vc);
	
	sortedMap.putAll(map);
	
	return sortedMap;
}


}


/*
 * Class for declaring a custom comparator for sorting the hashmap by values.
 */
class ValueComparator implements Comparator<String> {
	
	HashMap<String, Integer> map;
	
	public ValueComparator(HashMap<String, Integer> base){
		this.map = base;
	}
	
	@Override
	public int compare(String a,String b){
		if(map.get(a) >= map.get(b)){
			return -1;
		}
		else
			return 1;
	}
}
