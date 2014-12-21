package com.naveen.mapper;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

/*
 * This Class is for processing the input file which consists of user ids and ratings.
 */
public class DataScrubMapper
  extends Mapper<LongWritable, Text, Text, Text>
{
  Logger logger = Logger.getLogger(DataScrubMapper.class);
  
  protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
  {
    this.logger.info("Inside DataScrub Mapper class");
    


    //Below code splits the string into two strings of userids and ratings 
    String[] input = value.toString().split("\t");
    StringBuilder str = new StringBuilder(input[1]);
    str.append(":");
    str.append(input[2]);
    try
    {
      context.write(new Text(input[0]), new Text(str.toString()));
    }
    catch (IOException|InterruptedException e)
    {
      this.logger.info("Inside exception block. Something wrong in the context object");
      e.printStackTrace();
    }
  }
}