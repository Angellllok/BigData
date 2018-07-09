package rank;

import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public  class InitialGraphMapper extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
    
    public void map(LongWritable key,  Text Values,  Context context)
      throws  IOException,  InterruptedException
    {
  	
  	 //Setting up the format for XML line
       String line  = "<page>" +Values.toString() + "</page>";
       
       try{
      	 boolean outlinktrue = false;
      	 String title = getValue(line,"title").trim();
      	 String text = getValue(line,"text");
      	 // Using Regex expression for extract the links
      	 Matcher reg = Pattern.compile("\\[\\[(.*?)\\]\\]").matcher(text);
   		 while(reg.find())
   		 {
   			outlinktrue = true;
   			
   			context.write(new Text(title), new Text(reg.group(1).split("\\|")[0].trim()));
   		 }	        	 
   		 if   (!outlinktrue)
   	     {
   			context.write(new Text(title), new Text(Driver.NoOL));
   		 }
       }
       
       catch(Exception Exp){
      	  
       } 
   
 } 
    //XML Data extraction
    private static String getValue(String xml,String node) 
    		throws ParserConfigurationException, SAXException, IOException
    {
  	  
  	  String value = null;
        DocumentBuilderFactory fact = DocumentBuilderFactory.newInstance();
        
        DocumentBuilder build = fact.newDocumentBuilder();
        
        Document doc = build.parse(new InputSource(new StringReader(xml)));
        
        Element re = doc.getDocumentElement();
        
        NodeList nl = re.getElementsByTagName(node);
        if (nl != null && nl.getLength() > 0) 
        {
            NodeList sl = nl.item(0).getChildNodes();

            if (sl != null && sl.getLength() > 0)
            {
                value = sl.item(0).getNodeValue();
            }
        }
        
  	  return value;
  	  
    } 
    
 } 