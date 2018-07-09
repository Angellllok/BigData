package rank;

/* This program computes the page rank of the pages in wiki corpus */

import java.util.Properties;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;
 
public class Driver extends Configured
{
    public static String Number_of_pages = null;
     
	public static String NoOL = "####NoOutLinks####";
	public static Properties properties = new Properties();
  
  public static void main( String[] args) throws  Exception
  {
	  
	    
	  //Step 1: Finding total number of pages in wiki corpus
	  Counter counter = new Counter();	  
      ToolRunner .run(counter, args);
      System.out.println ("************   The Number of pages in wiki corpus are **************  " + Driver.Number_of_pages);
	  
	  
      //Step 2: Job for getting Initial Graph with page Ranks as 1/N  
	  InitialGraph linkGraph = new InitialGraph();	  
      ToolRunner .run(linkGraph, args);

      
      //Step 3: Repeating steps for 10 iterations and conversion of the rank
       for       (int iter=1; iter<11	;iter++)
      {
		      properties.setProperty("iteration", new Integer(iter).toString());
		      
		      RankProcessing rankprocess = new RankProcessing();
		      
	          ToolRunner .run(rankprocess, args);
	     
	   }
    
      //Step 4: filter and sort of the files
	   FilterSort filtersort = new FilterSort();	
	 
       ToolRunner .run(filtersort,args);	 
   
  }
  
}
  