package com.naveen.reducer;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MergeMatrixItemReducer
  extends Reducer<LongWritable, Text, LongWritable, Text>
{
  public void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, LongWritable, Text>.Context context)
  {
    StringBuilder str = new StringBuilder();
    
    Text preValue = new Text();
    for (Text value : values) {
      if (value.toString().substring(0, 2) == "I#")
      {
        str.append(value.toString());
        str.append(preValue.toString());
      }
      else
      {
        preValue = value;
        if (str.length() != 0) {
          str.append(value.toString());
        }
      }
    }
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
