package top.jolyoulu.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author: JolyouLu
 * @Date: 2022/11/13 20:37
 * @Version 1.0
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //遍历的是分组的
        System.out.println("============ GROPU"+key.getOrderId() + " ============");
        for (NullWritable value : values) {
            System.out.println(key);
        }
        context.write(key,NullWritable.get());
    }
}
