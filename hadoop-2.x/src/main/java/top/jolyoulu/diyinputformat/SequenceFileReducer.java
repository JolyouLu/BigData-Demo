package top.jolyoulu.diyinputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2022/9/25 16:29
 * @Version 1.0
 */
public class SequenceFileReducer extends Reducer<Text, BytesWritable,Text,BytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        //循环写出
        for (BytesWritable value : values) {
            context.write(key,value);
        }
    }
}
