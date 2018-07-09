package rank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CounterReducer extends Reducer<Text, IntWritable, Text, IntWritable>  //Taking the input from the mapper and summation of all the pages in wiki

{   
	 
	 @Override
	 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	 {		 
		  Integer sum = 0;
      //Calculating the total number of pages in wiki
   	  for (IntWritable k: values)
   	  {
   		    sum += k.get();
   	  }
   	  
  	  
   	   context.write(key,new IntWritable(sum));
		 
	 }
		 
		 


}