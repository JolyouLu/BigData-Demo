package top.jolyoulu.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2023/1/8 17:11
 * @Version 1.0
 */
public class TwoIndexReducer extends Reducer<Text,Text,Text,Text> {

    Text k = new Text();
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
//        收到
//        a--1.txt	4
//         --2.txt	2
//         --3.txt	1
//        输出
//        a 1.txt-->4 2.txt-->2 3.txt-->1
        StringBuilder sb = new StringBuilder();
        for (Text value : values) {
            sb.append(value.toString().replace("\t","-->")).append("\t");
        }
        k.set(key);
        v.set(sb.toString());

        context.write(k,v);
    }
}
