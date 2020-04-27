package cn.hut.mr.flowcount.flowcount;

import cn.hut.mr.flowcount.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description 输出数据处理类--接收Mapper的输出，默认根据键值对中键的数据进行排序，将key相同的值进行归并
 *
 * @author HZX
 * @date 2020/4/23 10:52
 */
public class FCReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean totalBean = new FlowBean();

    @Override
    protected void reduce(Text k2, Iterable<FlowBean> v2s, Context context)
            throws IOException, InterruptedException {
        // long sum_upFlow = 0;
        // long sum_downFlow = 0;
        // for (FlowBean flowBean : values) {
        // sum_upFlow += flowBean.getUpFlow();
        // sum_downFlow += flowBean.getDownFlow();
        // }
        for (FlowBean v2 : v2s) {
            totalBean.add(v2);
        }

        // 将数据封装到FlowBean对象
        context.write(k2, totalBean);
        totalBean.setDownFlow(0).setUpFlow(0);
    }
}
