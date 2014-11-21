package com.naveen.mapper;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

public class ItemVectorMapper
  extends Mapper<LongWritable, Text, Text, Text>
{
  static Logger logger = Logger.getLogger(ItemVectorMapper.class);
  
  protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
  {
    String line = value.toString();
    
    String[] input = line.split("\t");
    String[] inputRatings = input[1].split(";");
    String[] inputRatingComb = new String[2];
    

    StringBuilder str = new StringBuilder();
    String strOld = new String();
    str.append(input[0]);
    str.append(":");
    

    strOld = str.toString();
    for (int i = 0; i < inputRatings.length; i++)
    {
      inputRatingComb = inputRatings[i].split(":");
      str.append(inputRatingComb[1]);
      try
      {
        context.write(new Text(inputRatingComb[0]), new Text(str.toString()));
        str.replace(0, str.length(), strOld);
      }
      catch (IOException|InterruptedException e)
      {
        logger.info("In the exception block");
        e.printStackTrace();
      }
    }
  }
}
