package com.naveen.drivers;

import com.naveen.mapper.MergeMatrixItemMapper;
import com.naveen.reducer.MergeMatrixItemReducer;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class MergeMatrixItemDriver
  extends Configured
  implements Tool
{
  static Logger logger = Logger.getLogger(MergeMatrixItemDriver.class);
  
  public static void main(String[] args)
  {
    PropertyConfigurator.configure("/home/naveen/log4j.properties");
    
    
    args[0] = "/user/naveen/ItemVectors";
    
    args[1] = "/user/naveen/MegeVector";
    try
    {
      ToolRunner.run(new Configuration(), new MergeMatrixItemDriver(), args);
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
      Job job = new Job(conf, "Merge Matrix and Item Similarity - MapReduce Job");
      logger.info("Just Before creating job object");
      


      Path[] inputPath = new Path[2];
      inputPath[0] = new Path(args[0].trim());
      inputPath[1] = new Path("/user/naveen/Co-occurrenceVectors");
      logger.info("Naveen5:");
      

      Path outputPath = new Path(args[1].trim());
      logger.info("Args input directory:" + args[0]);
      logger.info("Args output directory:" + Arrays.toString(inputPath));
      

      logger.info("Before setting up the job");
      job.setJarByClass(MergeMatrixItemDriver.class);
      
      job.setMapperClass(MergeMatrixItemMapper.class);
      job.setReducerClass(MergeMatrixItemReducer.class);
      
      job.setMapOutputKeyClass(Text.class);
      
      job.setMapOutputValueClass(Text.class);
      
      job.setNumReduceTasks(9);
      
      FileSystem fs = FileSystem.get(conf);
      
      FileInputFormat.setInputPaths(job, inputPath);
      if (fs.exists(outputPath)) {
        fs.delete(outputPath, true);
      }
      FileOutputFormat.setOutputPath(job, outputPath);
      
      logger.info("Checking the job status");
      jobStatus = job.waitForCompletion(true);
      if (jobStatus)
      {
        logger.info("job is successful in creating the Matrix Merge and Item Similarity");
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
