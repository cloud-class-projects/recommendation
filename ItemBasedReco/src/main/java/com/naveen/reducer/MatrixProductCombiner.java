package com.naveen.reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/*
 * This class is used as a combiner for this MapReduce program for improving the performance
 * This combiner acts as a mini reducer where the output of the mapper are reduced in great deal before sending it to Reducer
 */
public class MatrixProductCombiner extends Reducer<Text, Text, Text, Text>{
	
	@Override
	  public void reduce(Text key, Iterable<Text> values, Context context)
	  {
		
		//System.out.println("In Combiner logic");
		Map<String, Integer> itemProduct = new HashMap<String, Integer>();
		
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
 		
 		
 		StringBuilder str = new StringBuilder();
 		
 		
 		for (Map.Entry<String, Integer> entry : itemProduct.entrySet()){
 			str.append(entry.getKey().toString());
 			str.append(":");
 			str.append(entry.getValue().toString());
 			//System.out.println("Value of the string:" + str.toString());
 			try {
				context.write(key, new Text(str.toString()));
				str.setLength(0);
				str.trimToSize();
			} catch (IOException | InterruptedException e) {
				str.setLength(0);
				str.trimToSize();
				e.printStackTrace();
			}
 		}
 		
	  }

}
