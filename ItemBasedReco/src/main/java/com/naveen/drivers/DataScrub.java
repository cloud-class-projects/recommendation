package com.naveen.drivers;

import com.naveen.mapper.DataScrubMapper;
import com.naveen.reducer.DataScrubReducer;
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

public class DataScrub
  extends Configured
  implements Tool
{
  static Logger logger = Logger.getLogger(DataScrub.class);
  
  public static void main(String[] args)
  {
    args[0] = "/user/naveen/Input";
    
    args[1] = "/user/naveen/UserVectors";
    try
    {
      ToolRunner.run(new Configuration(), new DataScrub(), args);
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
      Job job = new Job(conf, "DataScrubing - MapReduce Job");
      logger.info("Just Before creating job object");
      Path inputPath = new Path(args[0].trim());
      logger.info("Naveen5:");
      

      Path outputPath = new Path(args[1].trim());
      logger.info("Args input directory:" + args[0]);
      logger.info("Args output directory:" + args[1]);
      

      logger.info("Before setting up the job");
      job.setJarByClass(DataScrub.class);
      
      job.setMapperClass(DataScrubMapper.class);
      job.setReducerClass(DataScrubReducer.class);
      
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
        logger.info("job is successful creating the UserVector files");
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
