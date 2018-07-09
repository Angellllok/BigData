package rank;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class Counter extends Configured implements Tool 
{

// This counts the total number of pages present in the given wiki corpus
	 public int run(String[] args) throws Exception 
	 {
		    Path inPath = new Path(args[0]);
		    
		    Path outPath = new Path(args[ 1]+"/Num_of_pages");

		    Configuration conf = getConf(); 	
		    
		    
		    Job job  = Job .getInstance(conf, " TotalPages ");
		    job.setJarByClass( this .getClass());
		    FileOutputFormat.setOutputPath(job,  outPath);
		      

		    FileInputFormat.setInputPaths(job, inPath);
		    FileOutputFormat.setOutputPath(job, outPath);
		    job.setJobName("Counter");
		    job.setJarByClass(Counter.class);
		    job.setMapperClass(CounterMapper.class); // Mapper Class
		    job.setReducerClass(CounterReducer .class); // Reducer Class
		    job.setOutputKeyClass( Text .class);
		    job.setOutputValueClass( IntWritable .class);

		    
		  //Wait for the job completion
		      boolean output = job.waitForCompletion(true);
		      
		     
		     
		     //Read the number of pages in the wikipedia corpus
		      JobClient jc = new JobClient(conf);
		      FileSystem filesys = jc.getFs();
		      Path outputN = new Path(outPath+"/part-r-00000");
		      BufferedReader bufferrd = new BufferedReader(new InputStreamReader(filesys.open(outputN)));
		      String text_input = "";
		      String auxl = "";

		      while ((auxl = bufferrd.readLine()) != null) 
		      {
		              text_input += auxl;
		      }     

		      Driver.Number_of_pages = text_input.split("\t")[1];
		      
		      bufferrd.close();
		      filesys.delete(outPath, true);
		      filesys.close();
		      jc.close();
		      return output==true?0:1;
		   }
}
	 