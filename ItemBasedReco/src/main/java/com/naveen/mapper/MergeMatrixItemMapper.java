package com.naveen.mapper;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MergeMatrixItemMapper
  extends Mapper<LongWritable, Text, LongWritable, Text>
{
  protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
  {
    
	//finding the file name from which the split is being processed. This is used to determine the append criteria for writing into context objects  
	FileSplit fileSplit = (FileSplit)context.getInputSplit();
    
    String filePath = fileSplit.getPath().getName();
    




    StringBuilder str = new StringBuilder();
    if (filePath == "/user/naveen/ItemVectors") {
      str.append("I#");
    } else {
      str.append("M#");
    }
    str.append(value.toString());
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
