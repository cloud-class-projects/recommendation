package com.naveen.mapper;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class CoOccurrenceMapper
  extends Mapper<LongWritable, Text, Text, Text>
{
  Logger logger = Logger.getLogger(CoOccurrenceMapper.class);
  
  protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
  {
    PropertyConfigurator.configure("/home/naveen/log4j.properties");
    String line = value.toString();
    String[] inputValue = line.split("\t");
    
    this.logger.info("Printing InputValue0" + inputValue[0]);
    this.logger.info("Printing InputValue1" + inputValue[1]);
    System.out.println("Printing in printout:" + inputValue[1]);
    String[] input = inputValue[1].split(";");
    String[] itemIdRating = new String[2];
    String[] itemId = new String[input.length];
    for (int i = 0; i < input.length; i++)
    {
      this.logger.info("Input String" + input[i]);
      itemIdRating = input[i].split(":");
      

      itemId[i] = itemIdRating[0];
      this.logger.info("ItemId String:" + itemId[i]);
    }
    for (int i = 0; i < itemId.length; i++) {
      for (int j = i + 1; j < itemId.length; j++) {
        try
        {
          context.write(new Text(itemId[i]), new Text(itemId[j]));
          context.write(new Text(itemId[j]), new Text(itemId[i]));
        }
        catch (IOException|InterruptedException e)
        {
          this.logger.info("Failed in writing the context objects");
          e.printStackTrace();
        }
      }
    }
  }
}
