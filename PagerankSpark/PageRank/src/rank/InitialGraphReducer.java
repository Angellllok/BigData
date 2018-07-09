package rank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class InitialGraphReducer extends Reducer<Text ,  Text ,  Text ,  Text > {
	      @Override 
	      public void reduce( Text key,  Iterable<Text > counts,  Context context)
	         throws IOException,  InterruptedException {   
	    	  //Setting up the initial page rank as suggested (1/N)
	    	  String val = new Double(1.0/Double.parseDouble(context.getConfiguration().get("N"))).toString();
	    	  for(Text txt:counts){
	    		  val += ","+txt.toString();
	    	  }
	    	  
	    	  context.write(key, new Text(val));
	         
	      }
   }


