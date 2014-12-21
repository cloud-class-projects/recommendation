package com.naveen.drivers;



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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.naveen.mapper.MatrixProductMapper;
import com.naveen.reducer.MatrixProductCombiner;
import com.naveen.reducer.MatrixProductReducer;

public class MatrixProductDriver
  extends Configured
  implements Tool
{
  static Logger logger = Logger.getLogger(MatrixProductDriver.class);
  
  public static void main(String[] args)
  {

	  
    try
    {
      ToolRunner.run(new Configuration(), new MatrixProductDriver(), args);
    }
    catch (Exception e)
    {
      e.getStackTrace();
    }
  }
  
  public int run(String[] args)
    throws Exception
  {
	  
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
			
		    args[0] = prop.getProperty("MergeVectorOutput");
		    
		    args[1] = prop.getProperty("ProductOutput");
		    


		}
		
		catch(Exception e){
			e.printStackTrace();
		}
	  
	  
	  
    boolean jobStatus = false;
    logger.info("Inside the run method");
    
    Configuration conf = getConf();
    try
    {
      Job job = new Job(conf, "Matrix Product - MapReduce Job");
      logger.info("Just Before creating job object");
      


      Path inputPath = new Path(args[0].trim());
      logger.info("Naveen5:");
      

      Path outputPath = new Path(args[1].trim());
      logger.info("Args input directory:" + args[0]);
      

      logger.info("Before setting up the job");
      job.setJarByClass(MatrixProductDriver.class);
      
      
      job.setMapperClass(MatrixProductMapper.class);
      job.setReducerClass(MatrixProductReducer.class);
      job.setCombinerClass(MatrixProductCombiner.class);
      
      job.setMapOutputKeyClass(Text.class);
      
      job.setMapOutputValueClass(Text.class);
      
      job.setOutputKeyClass(Text.class);
      
      job.setOutputValueClass(Text.class);
      
      job.setNumReduceTasks(6);
      
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
        logger.info("job is successful in creating the Matrix Product");
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
