package com.naveen.reducer;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.log4j.Logger;

public class ItemVectorReducer
  extends Reducer<Text, Text, Text, Text>
{
  static Logger logger = Logger.getLogger(ItemVectorReducer.class);
  
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
      context.write(key, new Text(str.toString()));
    }
    catch (IOException|InterruptedException e)
    {
      logger.info("Failed while writing Context object");
      e.printStackTrace();
    }
  }
}
