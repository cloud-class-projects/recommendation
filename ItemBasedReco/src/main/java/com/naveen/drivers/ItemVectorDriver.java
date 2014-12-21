package com.naveen.drivers;

import com.naveen.mapper.ItemVectorMapper;
import com.naveen.reducer.ItemVectorReducer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class ItemVectorDriver
  extends Configured
  implements Tool
{
  static Logger logger = Logger.getLogger(ItemVectorDriver.class);
  
  public static void main(String[] args)
  {
    
	
    try
    {
      ToolRunner.run(new Configuration(), new ItemVectorDriver(), args);
    }
    catch (Exception e)
    {
      e.getStackTrace();
    }
  }
  
  public int run(String[] args)
    throws Exception
  {
    boolean jobStatus = false;
    logger.info("Inside the run method");
    
    
    
    //Load the property file which has the input and output file directory names
    
	Properties prop = new Properties();
	
	String propFileName = "/Parameters.properties";
	
	InputStream inputStream = null;
	
	try{
	    logger.info("Inside the try block");
	    //inputStream = new FileInputStream(propFileName);
		
		inputStream = getClass().getResourceAsStream(propFileName);
		
		logger.info("Naveen1:" + inputStream);
		prop.load(inputStream);
		
		logger.info("Naveen2");
		
	    args[0] = prop.getProperty("DataScrubOutput");
	    
	    args[1] = prop.getProperty("ItemVectorOutput");
	    


	}
	
	catch(Exception e){
		e.printStackTrace();
	}
	
	
	
    Configuration conf = getConf();
    try
    {
      Job job = new Job(conf, "ItemVectorDriver - MapReduce Job");
      logger.info("Just Before creating job object");
      Path inputPath = new Path(args[0].trim());
      logger.info("Naveen5:");
      

      Path outputPath = new Path(args[1].trim());
      logger.info("Args input directory:" + args[0]);
      logger.info("Args output directory:" + args[1]);
      

      logger.info("Before setting up the job");
      job.setJarByClass(ItemVectorDriver.class);
      
      job.setMapperClass(ItemVectorMapper.class);
      job.setReducerClass(ItemVectorReducer.class);
      
      job.setMapOutputKeyClass(Text.class);
      
      job.setMapOutputValueClass(Text.class);
      
      MultipleOutputs.addNamedOutput(job, "ItemVector", TextOutputFormat.class, TextOutputFormat.class, TextOutputFormat.class);
      
      job.setNumReduceTasks(4);
      
      FileSystem fs = FileSystem.get(conf);
      
      FileInputFormat.setInputPaths(job, new Path[] { inputPath });
      if (fs.exists(outputPath)) {
        fs.delete(outputPath, true);
      }
      FileOutputFormat.setOutputPath(job, outputPath);
      
      logger.info("Checking the job status");
      jobStatus = job.waitForCompletion(true);
      if (jobStatus)
      {
        logger.info("job is successful in creating the ItemVectors");
        return 0;
      }
      return 1;
    }
    catch (IOException e)
    {
      return 1;
    }
    catch (InterruptedException e)
    {
      return 1;
    }
    catch (ClassNotFoundException e) {}
    return 1;
  }
}
