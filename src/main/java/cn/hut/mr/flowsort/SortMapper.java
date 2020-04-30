package cn.hut.hdfs_demo.mr.flowsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {
    private FlowBean k2 = new FlowBean();
    private NullWritable v2 = NullWritable.get();
//    private Text text = new Text();
//
//    @Override
//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        String[] split = value.toString().split("\t");
//        text.set(split[0]);
//        flowBean.set(Long.parseLong(split[1]), Long.parseLong(split[2]), Long.parseLong(split[3]));
//
//        context.write(flowBean, text);
//    }

    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        String[] value_arr = v1.toString().split("\t");
        String telno = value_arr[0];
        long upload = Long.parseLong(value_arr[1]);
        long download = Long.parseLong(value_arr[2]);
        long sumload = Long.parseLong(value_arr[3]);
        k2.setTelno(telno).setUpload(upload).setDownload(download).setSumload(sumload);
        context.write(k2, v2);
    }
}
