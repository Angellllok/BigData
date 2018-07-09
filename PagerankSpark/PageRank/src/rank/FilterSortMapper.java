package rank;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public  class FilterSortMapper extends Mapper<LongWritable ,  Text ,  DoubleWritable ,  Text > {
    
    public void map(LongWritable offset,  Text lineText,  Context context)
      throws  IOException,  InterruptedException {
    	// Mapper job for filtering and sorting
    	//Read Line
	   	 String lineStr = lineText.toString();
    	 int tab_i = lineStr.indexOf("\t");
    	 String page_wiki = lineStr.substring(0,tab_i);
    	 String pgrnk = lineStr.substring(tab_i+1).split(",")[0];
    	     	 
         //Storing the outgoing links present
    	 context.write(new DoubleWritable(Double.parseDouble(pgrnk)),new Text(page_wiki));
  }
     
     
  
} 

