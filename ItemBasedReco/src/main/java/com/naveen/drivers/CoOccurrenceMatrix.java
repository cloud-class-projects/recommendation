package com.naveen.drivers;

import com.naveen.mapper.CoOccurrenceMapper;
import com.naveen.reducer.CoOccurrenceReducer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class CoOccurrenceMatrix extends Configured implements Tool{
  static Logger logger = Logger.getLogger(CoOccurrenceMatrix.class);
  
  public static void main(String[] args)
  {
    
	//Loading the log4j property file
	PropertyConfigurator.configure("/home/naveen/log4j.properties");
    
	//Input file location
    args[0] = "/user/naveen/UserVectors";
    
    //Output file location
    args[1] = "/user/naveen/Co-occurrenceVectors";
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
      
      job.setNumReduceTasks(9);
      
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
