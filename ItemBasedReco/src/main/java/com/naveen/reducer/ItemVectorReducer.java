package com.naveen.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

public class ItemVectorReducer
  extends Reducer<Text, Text, Text, Text>
{
  private MultipleOutputs multiOutput;
  
  static Logger logger = Logger.getLogger(ItemVectorReducer.class);
  
  
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
    StringBuilder str = new StringBuilder();
    for (Text value : values)
    {
      str.append(value.toString());
      str.append(";");
    }
    try
    {
      multiOutput.write("ItemVector",key, new Text(str.toString()));
    }
    catch (IOException|InterruptedException e)
    {
      logger.info("Failed while writing Context object");
      e.printStackTrace();
    }
  }
}
