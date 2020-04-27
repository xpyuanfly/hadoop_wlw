package cn.hut.mr.flowcount.flowcount;

import cn.hut.mr.flowcount.bean.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description 输入数据处理类
 *
 * @author HZX
 * @date 2020/4/23 10:35
 */
public class FCMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private FlowBean flowBean = new FlowBean();
    private Text text = new Text();

    // 这个方法，当从文件中读取一行数据，该方法就会执行一次
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 将value转为字符串类型
        String lineStr = value.toString();
        // 将linStr分割为各个字段，获得一个字符串数组
        String[] fields = lineStr.split("\t");
        // 取出我们需要的字段
        // 1.取出手机号
        String phone = fields[1];
        // 2.取出上行流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        text.set(phone);
        flowBean.setUpFlow(upFlow).setDownFlow(downFlow);

        context.write(text, flowBean);
    }
}
