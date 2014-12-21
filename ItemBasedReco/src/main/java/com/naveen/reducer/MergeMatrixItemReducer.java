package com.naveen.reducer;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MergeMatrixItemReducer
  extends Reducer<Text, Text, Text, Text>
{
  
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
  {
    StringBuilder str = new StringBuilder();
    
    System.out.println("Inside the Reducer Class");
    Text preValue = new Text();
    for (Text value : values) {
    	
      System.out.println("Printing the substring value from the if statement" + value.toString().substring(0,2));
      
      if (value.toString().substring(0, 2).equals("I#"))
      {
    	System.out.println("Inside if in the reducer");
        str.append(value.toString());
        
        str.append("@");
        if(preValue.getLength()!=0)
        	str.append(preValue.toString());
        
        System.out.println("Value string to be printed:" + value.toString());
      }
      else if(value.toString().substring(0, 2).equals("M#"))
      {
    	System.out.println("Inside the second else if in the reducer");
        preValue.set(value);
        if (str.length() != 0) {
          str.append(value.toString());
        }
        
        System.out.println("Value string in the else statement:" + value.toString());
      }
    }
    System.out.println("Reducer string:" + str.toString());
    try
    {
      context.write(key, new Text(str.toString()));
    }
    catch (IOException|InterruptedException e)
    {
      e.printStackTrace();
    }
  }
}
