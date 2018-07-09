package rank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


public class InitialGraph extends Configured implements Tool {

   public int run( String[] args) throws  Exception {
	  
	   Configuration conf = getConf();
	   conf.set("N", Driver.Number_of_pages);
	   
	  //Start the Map Reduce job
      Job job  = Job .getInstance(getConf(), " LinkGraph ");
      job.setJarByClass( this .getClass());	

      
      //Defining the inout and output paths for the files
      FileInputFormat.addInputPaths(job,  args[0]);
      
      String output = args[1]+"/InitialGraph";
      FileOutputFormat.setOutputPath(job,  new Path(output));
      
      //Mapper and Reducer Jobs 
      job.setMapperClass(InitialGraphMapper.class); //Mapper Job
      job.setReducerClass(InitialGraphReducer.class); // Reducer Job
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( Text .class);

      //Wait for job completion
      return job.waitForCompletion( true)  ? 0 : 1;
   }
}
