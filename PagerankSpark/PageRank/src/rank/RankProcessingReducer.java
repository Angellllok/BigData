package rank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RankProcessingReducer extends Reducer<Text ,  Text ,  Text ,  Text > {
		      @Override 
		      public void reduce( Text key,  Iterable<Text > counts,  Context context)
		         throws IOException,  InterruptedException {   
		    	  
		    	  String split_1 = "####"; 
		    	  Double dampfact = 0.85;   //Setting up Damping Factor as suggested
		    	  String outLinks = null;
		    	  String pageRank = null;
		    	  Double Calc = 0.0;
		    	  
		    	  //Calculating the new page rank
		    	  
		    	  //for each value of this particular key
		    	  for(Text data: counts)
		    	  {
		    		  String str = data.toString();
		    		  
		    		  //If a marker line, then just store them as outlinks;
		    		  if (str.startsWith("@@"))
		    		  {
		    			  outLinks = str.substring(2);
		    		  }
		    		  
		    		  //calculate the page rank/Cardinality for each value.
		     		  else
		     		  {
		    			  Calc += Double.parseDouble(str.split(split_1)[0]) / Double.parseDouble(str.split(split_1)[1]);
		    		  }
		     	  } 
		    	  
		    	  //Ignore if page not present 
		    	  if (outLinks == null)
		    	  {
		    		  return;
		    	  }
		        	        	
		    	  	//page rank computation.
		        	  Double  pgrnk = ((1-dampfact)) + (dampfact * Calc);	        	  	        	  
		        	  pageRank = pgrnk.toString();
		        	  outLinks = outLinks.substring(outLinks.indexOf(",")+1);
		        	  
		        	  //Add new page rank and outlinks and write it to the context
		       		  context.write(key, new Text(pageRank +","+outLinks));
		        	  
		          }
}


