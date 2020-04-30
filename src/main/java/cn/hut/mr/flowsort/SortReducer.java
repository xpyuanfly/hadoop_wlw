package cn.hut.hdfs_demo.mr.flowsort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<FlowBean, NullWritable, FlowBean, NullWritable> {
//    @Override
//    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        for (Text value : values) {
//            context.write(value, key);
//        }
//    }

    @Override
    protected void reduce(FlowBean k2, Iterable<NullWritable> v2s, Context context) throws IOException, InterruptedException {
        context.write(k2, NullWritable.get());
    }
}
