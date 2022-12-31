package top.jolyoulu.diyinputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2022/9/25 16:27
 * @Version 1.0
 */
public class SequenceFileMapper extends Mapper<Text, BytesWritable,Text, BytesWritable> {
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
