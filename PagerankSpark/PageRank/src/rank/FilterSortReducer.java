package rank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Reducer;

       //Reducer class for filter and sort
	   public class FilterSortReducer extends Reducer<DoubleWritable ,  Text ,  Text ,  DoubleWritable > {
		      @Override 
		      public void reduce( DoubleWritable key,  Iterable<Text > counts,  Context context)
		         throws IOException,  InterruptedException {   
		    	  
		    	   for (Text page: counts){
		    		   context.write(page, (key));   
		    	   } 
		    	   
		      }  
		   }
		   //Sorting the contents in the desired order
		   class DoubleDescComp extends WritableComparator {

		 		//Constructor		 
		 		protected DoubleDescComp() {
		 			super(DoubleWritable.class, true);
		 		}
		 		
		 		//Suppress warning
		 		@SuppressWarnings("rawtypes")

		 		
		 		@Override
		 		public int compare(WritableComparable var1, WritableComparable var2) 
		 		{
		 			DoubleWritable var3 = (DoubleWritable)var1;
		 				 			
		 			DoubleWritable var4 = (DoubleWritable)var2;
		 			
		 			//Multiplying by -1 for reverse sort
		 			return -1 * var3.compareTo(var4);
		 		}
		 	}
