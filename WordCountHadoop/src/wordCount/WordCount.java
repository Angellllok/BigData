package wordCount;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class WordCount {
	
		public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(wordCount.WordCount.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		// input and output directories (not files)
		FileInputFormat.addInputPath(conf, new Path("input"));
		FileOutputFormat.setOutputPath(conf, new Path("output"));
		conf.setMapperClass(wordCount.WordCountMapper.class);
		conf.setReducerClass(wordCount.WordCountReducer.class);
		conf.setCombinerClass(wordCount.WordCountReducer.class);
		client.setConf(conf);
		try {
		JobClient.runJob(conf);
		} catch (Exception e) {
		e.printStackTrace();
		}
		}
		}