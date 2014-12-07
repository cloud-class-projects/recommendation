package com.naveen.reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MatrixProductReducer extends Reducer<Text, Text, Text, Text>{
	protected void Reduce(Text key, Iterable<Text> values, Context context){
		
		
		System.out.println("In Reducer");
		Map<String, Integer> itemProduct = new HashMap<String, Integer>();
		
		String [] itemDetail = new String[2];
		
 		for(Text value:values){
 			itemDetail = value.toString().split(":");
 			
 			System.out.println("item detail array:" + Arrays.toString(itemDetail));
 			
 			if (itemProduct.get(itemDetail[0]) ==null){
 				System.out.println("Record is not present:" + itemDetail[0]);
 				System.out.println("ItemProduct Size in the else clause:" + itemProduct.size());
 				if(itemProduct.size() < 11){
 				  itemProduct.put(itemDetail[0], Integer.parseInt(itemDetail[1]));
 				}
 			}
 			else{
 				System.out.println("Record is present:" + itemDetail[0]);
 				System.out.println("ItemProduct Size in the if clause:" + itemProduct.size());
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
 		      str.append(";");
 		}
 		
 		System.out.println("Value of the string:" + str.toString());
 		try {
			context.write(key, new Text(str.toString()));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

}
