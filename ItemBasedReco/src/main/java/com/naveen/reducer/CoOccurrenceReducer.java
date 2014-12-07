package com.naveen.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class CoOccurrenceReducer
  extends Reducer<Text, Text, Text, Text>
{
	
  private MultipleOutputs multiOutput;
  
  Logger logger = Logger.getLogger(CoOccurrenceReducer.class);
  
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
   * This method is for creating the multiOutputs object which would be used for writing the output key value pairs
   */
  public void setup(Context context){
	  
	  multiOutput = new MultipleOutputs(context);
  }
  
  
  public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
  {
    PropertyConfigurator.configure("/home/naveen/log4j.properties");
    this.logger.info("Inside Reducer logic for CoOccurrence Matrix");
    
    Map<String, Integer> reduceValues = new HashMap<String,Integer>();
    int i;
    for (Text value : values) {
      if (reduceValues.get(value.toString()) == null)
      {
        reduceValues.put(value.toString(), Integer.valueOf(1));
      }
      else
      {
        i = reduceValues.get(value.toString()).intValue();
        reduceValues.put(value.toString(), Integer.valueOf(i + 1));
      }
    }
    StringBuilder str = new StringBuilder();
    for (Object entry : reduceValues.entrySet())
    {
      str.append((String)((Map.Entry)entry).getKey());
      str.append(":");
      str.append(((Integer)((Map.Entry)entry).getValue()).toString());
      str.append(";");
    }
    try
    {
      this.logger.info("Context object:" + str);
      multiOutput.write("Co",key, new Text(str.toString()));
    }
    catch (IOException|InterruptedException e)
    {
      this.logger.info("Failure during writing context object");
      e.printStackTrace();
    }
  }
}
