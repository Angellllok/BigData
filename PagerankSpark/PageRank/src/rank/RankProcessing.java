package rank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class RankProcessing extends Configured implements Tool {
	

		   public int run( String[] args) throws  Exception {
			   
			   Configuration conf = getConf();
			   conf.set("N", Driver.Number_of_pages);
		
		      Job job  = Job .getInstance(getConf(), " RankProcessing ");
		      job.setJarByClass( this .getClass());
		      
		      //Input and output paths for files
		      
		      String input = null;
		      String output = null;
		      int curr_iter = Integer.parseInt(Driver.properties.getProperty("iteration"));
		      int prev_iter = curr_iter - 1;
		      if (prev_iter < 1){
		    	input = args[1]+"/InitialGraph";        
		      }
		      else
		      {
		    	  input = args[1] + "/iteration_"+prev_iter;
		      }
		      
		      	      
		      FileInputFormat.addInputPaths(job,  input);
		      
		      output = args[1]+"/iteration_"+curr_iter;
		      FileOutputFormat.setOutputPath(job,  new Path(output));
		      
		      //Mapper and Reducer Jobs
		      job.setMapperClass(RankProcessingMapper.class); //Mapper
		      job.setReducerClass(RankProcessingReducer.class); //Reducer
		      job.setOutputKeyClass(Text.class);
		      job.setOutputValueClass(Text.class);

		      //Wait for job completion
		      return job.waitForCompletion( true)  ? 0 : 1;
		   }
}