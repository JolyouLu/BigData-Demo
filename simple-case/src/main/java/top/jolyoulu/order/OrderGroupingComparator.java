package top.jolyoulu.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author: JolyouLu
 * @Date: 2022/11/13 21:08
 * @Version 1.0
 */
public class OrderGroupingComparator extends WritableComparator {

    protected OrderGroupingComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
//        1	222.8
//        1	32.4
//        2	100.4
//        2	32.4
//        2	5.3
//        3	323.1
//        3	123.4

        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        //只要id相同就认为是相同的key
        int res;
        if (aBean.getOrderId() > bBean.getOrderId()){
            res = 1;
        }else if (aBean.getOrderId() < bBean.getOrderId()){
            res = -1;
        }else {
            res = 0;
        }
        return res;
    }

}
