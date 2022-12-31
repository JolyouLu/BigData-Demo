package top.jolyoulu.mapjion;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: JolyouLu
 * @Date: 2022/11/20 18:13
 * @Version 1.0
 */
public class DistributedMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    Map<String, String> pdMap = new HashMap<>();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //缓存小表
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){
            //切割
            String[] fileds = line.split(" ");
            pdMap.put(fileds[0],fileds[1]);
        }
        //关闭资源
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
        //jion合并数据
        //获取一行数据
        String line = value.toString();
        //切割
        String[] fileds = line.split(" ");
        //获取pid
        String pid = fileds[1];
        //获取pname
        String pname = pdMap.get(pid);
        //拼接，写出
        k.set(fileds[0] + " " + pname + " " + fileds[2]);
        context.write(k,NullWritable.get());
    }
}
