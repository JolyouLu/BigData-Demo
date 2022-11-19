package top.jolyoulu.diyoutputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2022/11/19 16:14
 * @Version 1.0
 */
public class FilterReducer extends Reducer<Text, NullWritable,Text,NullWritable> {

    Text k = new Text();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable nullWritable : values) {
            String line = key.toString();
            line += "\r\n";
            k.set(line);
            context.write(k,NullWritable.get());
        }
    }
}
