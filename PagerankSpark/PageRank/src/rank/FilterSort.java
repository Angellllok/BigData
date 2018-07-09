package rank;



import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;


public class FilterSort extends Configured implements Tool {

   public int run( String[] args) throws  Exception {
	  
	  //Start the Map Reduce job
      Job job  = Job .getInstance(getConf(), "FilterSort");
      job.setJarByClass(this.getClass());
      job.setSortComparatorClass(DoubleDescComp.class);

      int Last_iter = Integer.parseInt(Driver.properties.getProperty("iteration"));
          
      //Set the input and output paths for the iteration and final filter sort file
      String input = args[1] + "/iteration_"+Last_iter;
     
      String output = args[1]+"/filtersort";	
      FileInputFormat.addInputPaths(job,  input);
      FileOutputFormat.setOutputPath(job,  new Path(output));
      
      //Mapper and Reducer jobs
      job.setMapperClass(FilterSortMapper.class);      //Mapper 
      job.setReducerClass(FilterSortReducer.class);    //Reducer
      job.setOutputKeyClass( DoubleWritable .class);
      job.setOutputValueClass(Text.class);

      //Wait for job completion
      return job.waitForCompletion(true)  ? 0 : 1;
      
    } 
   
}