

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TigerReducer extends Reducer<Text, Text, Text, Text> {

    private static int RECORD_LEN = 26;
    private String[] container = new String[RECORD_LEN];

    private void container_modifier(String string) {
        String[] tmp_container = string.split("#");
        for(int i = 0; i < RECORD_LEN; i++)
            if (!(tmp_container[i].equals("null") || tmp_container[i].equals(""))) container[i] = tmp_container[i];
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) container_modifier(value.toString());
        context.write(key, new Text(String.join("#", container)));
    }
}