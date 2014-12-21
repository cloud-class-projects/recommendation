package com.naveen.drivers;

import com.naveen.mapper.CoOccurrenceMapper;
import com.naveen.reducer.CoOccurrenceReducer;

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


public class CoOccurrenceMatrix extends Configured implements Tool{
  static Logger logger = Logger.getLogger(CoOccurrenceMatrix.class);
  
  public static void main(String[] args)
  {
    
	  
    try
    {
      ToolRunner.run(new Configuration(), new CoOccurrenceMatrix(), args);
    }
    catch (Exception e)
    {
      e.getStackTrace();
    }
  }
  
  public int run(String[] args)
    throws Exception
  {
	  
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
		    
		    args[1] = prop.getProperty("CoOccurrenceOutput");
		    


		}
		
		catch(Exception e){
			e.printStackTrace();
		}
	  
	  
	  
    boolean jobStatus = false;
    logger.info("Inside the run method");
    
    Configuration conf = getConf();
    try
    {
      Job job = new Job(conf, "CoOccurrenceMatrix - MapReduce Job");
      logger.info("Just Before creating job object");
      Path inputPath = new Path(args[0].trim());
      logger.info("Naveen5:");
      

      Path outputPath = new Path(args[1].trim());
      logger.info("Args input directory:" + args[0]);
      logger.info("Args output directory:" + args[1]);
      

      logger.info("Before setting up the job");
      
      //Mentioning all the job level configuration settings
      job.setJarByClass(DataScrub.class);
      
      job.setMapperClass(CoOccurrenceMapper.class);
      job.setReducerClass(CoOccurrenceReducer.class);
      
      job.setMapOutputKeyClass(Text.class);
      
      job.setMapOutputValueClass(Text.class);
      
      job.setNumReduceTasks(4);
      
      MultipleOutputs.addNamedOutput(job, "Co", TextOutputFormat.class, TextOutputFormat.class, TextOutputFormat.class);
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
        logger.info("job is successful in creating the CoOccurrence Matrix");
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
