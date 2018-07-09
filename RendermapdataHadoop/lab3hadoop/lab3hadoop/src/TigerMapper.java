

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TigerMapper extends Mapper<Object, Text, Text, Text> {

    private static int RECORD_LEN = 26;
    private String[] container = new String[RECORD_LEN];

    public void parse_record(String string) {
        String first_char = string.substring(0, 1);
        if (first_char.equals("1")) {
            parse_type1(string);
        } else if (first_char.equals("2")) {
            parse_type2(string);
        }
    }

    public void parse_type1(String string) {
        container[0] = string.substring(19, 49).trim(); // Name
        container[1] = string.substring(49, 53).trim(); // Type
        container[2] = string.substring(190, 200).trim(); // Start
        container[3] = string.substring(200, 209).trim(); //
        container[4] = string.substring(209, 219).trim(); // End
        container[5] = string.substring(219, 228).trim();
    }

    public void parse_type2(String string) {
        container[6] = string.substring(18, 28).trim(); // 1
        container[7] = string.substring(28, 37).trim();
        container[8] = string.substring(37, 47).trim(); // 2
        container[9] = string.substring(47, 56).trim();
        container[10] = string.substring(56, 66).trim(); // 3
        container[11] = string.substring(66, 75).trim();
        container[12] = string.substring(75, 85).trim(); // 4
        container[13] = string.substring(85, 94).trim();
        container[14] = string.substring(94, 104).trim(); // 5
        container[15] = string.substring(104, 113).trim();
        container[16] = string.substring(113, 123).trim(); // 6
        container[17] = string.substring(123, 132).trim();
        container[18] = string.substring(132, 142).trim(); // 7
        container[19] = string.substring(142, 151).trim();
        container[20] = string.substring(151, 161).trim(); // 8
        container[21] = string.substring(161, 170).trim();
        container[22] = string.substring(170, 180).trim(); // 9
        container[23] = string.substring(180, 189).trim();
        container[24] = string.substring(189, 199).trim(); // 10
        container[25] = string.substring(199, 208).trim();
    }

    public static String get_tlid(String string) {
        return string.substring(5, 15).trim();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        parse_record(string);
        context.write(new Text(get_tlid(string)), new Text(String.join("#", container)));
    }
}
