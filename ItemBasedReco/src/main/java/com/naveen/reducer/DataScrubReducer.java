package com.naveen.reducer;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

public class DataScrubReducer
  extends Reducer<Text, Text, Text, Text>
{
  Logger logger = Logger.getLogger(DataScrubReducer.class);
  
  public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
  {
    this.logger.info("Inside Reducer class");
    StringBuilder str = new StringBuilder();
    for (Text val : values)
    {
      str.append(val.toString());
      str.append(";");
    }
    try
    {
      context.write(key, new Text(str.toString()));
    }
    catch (IOException|InterruptedException e)
    {
      this.logger.info("Something wrong in the context object in the reducer");
      e.printStackTrace();
    }
  }
}
