package rank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RankProcessingMapper extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
    
	   
	   
    public void map(LongWritable key,  Text Values,  Context context)
      throws  IOException,  InterruptedException {
  	
  	 
  	 String split_1 = "####";
  	 
  	 //Read the line and get the pageRank and outlinks
  	 String lineStr = Values.toString();
  	 int tab_i = lineStr.indexOf("\t");
  	 String page = lineStr.substring(0,tab_i);
  	 String value = lineStr.substring(tab_i+1);
  	 int firstComm_Index = value.indexOf(",");
  	 String oldPageRank = value.substring(0, firstComm_Index);
  	 String outlinks = value.substring(firstComm_Index+1);
  	 
  	 
  	 //If there are outlinks
  	 if (!outlinks.equals(Driver.NoOL)){    	  	
  		 int totalOutlinks=outlinks.length();
  		 
  		 //for each outlink 
  		 for (String outlink: outlinks.split(","))
  		 {
  			 
  			 //write current page rank and total outlink count of the input page so that it can be used for page rank computation.
  			 context.write(new Text(outlink), new Text(oldPageRank+split_1+totalOutlinks));
	    	 }
  	 } 
  	 
     
  	 //write a marker entry with original outlinks to save the output in the same format as the input.
  	context.write(new Text(page),new Text("@@"+value));
    }
       
       
    
 }
