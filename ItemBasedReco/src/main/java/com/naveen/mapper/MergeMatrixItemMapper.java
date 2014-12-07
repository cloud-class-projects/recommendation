package com.naveen.mapper;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MergeMatrixItemMapper
  extends Mapper<LongWritable, Text, Text, Text>
{
  protected void map(LongWritable key, Text value, Context context)
  {
    
	//Creating the split required
	  
	String inputStr [] = new String[2];
	
	inputStr = value.toString().split("\t");
	
	//finding the file name from which the split is being processed. This is used to determine the append criteria for writing into context objects  
	FileSplit fileSplit = (FileSplit)context.getInputSplit();
    
    String filePath = fileSplit.getPath().getName();
    
    
    /*
     * Below logic is to Distinguish between the Item and CoOccurrence Vectors.
     */

    System.out.println("Mapper String:" + filePath);
    try{
    	
    
    StringBuilder str = new StringBuilder();
    if (filePath.contains("ItemVector")) {
      System.out.println("Inside If statement");
      str.append("I#");
      str.append(inputStr[1].toString());
      System.out.println("Printing String to be printed");
      context.write(new Text(inputStr[0]), new Text(str.toString()));
    } else if(filePath.contains("Co")) {
    	System.out.println("Inside else statement");
      str.append("M#");
      str.append(inputStr[1].toString());
      context.write(new Text(inputStr[0]), new Text(str.toString()));
    }
    
    
    }
    catch (IOException|InterruptedException e)
    {
      e.printStackTrace();
    }
  }
}
