package top.jolyoulu.log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2022/11/26 14:34
 * @Version 1.0
 */
public class LogMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //解析数据
        boolean result = paresLog(line,context);
        if (!result){
            return;
        }
        //解析通过，写数据
        context.write(value,NullWritable.get());
    }

    private boolean paresLog(String line, Context context) {
        String[] fields = line.split(" ");
        if (fields.length > 11){
            //计数器
            context.getCounter("map","true").increment(1);
            return true;
        }else {
            //计数器
            context.getCounter("map","false").increment(1);
            return false;
        }
    }
}
