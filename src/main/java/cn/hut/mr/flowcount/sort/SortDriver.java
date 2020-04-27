package cn.hut.mr.flowcount.sort;

import cn.hut.mr.flowcount.bean.FlowBean;
import cn.hut.mr.flowcount.constants.FlowConstant;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortDriver{
    public static void main(String[] args) {
        try{
            // 构建Configuration对象
            Configuration conf = new Configuration();

            Path outPath = new Path(FlowConstant.OUTPUT_PATH_sort);
            // 判断输出文件是否已经存在，存在的话就将其删除
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }

            // 构建一个Job对象
            Job job = Job.getInstance(conf);

            // 设置job所用的类在哪个jar包
            job.setJarByClass(SortDriver.class);
            // 设置Mapper 和 Reducer所在的类
            job.setMapperClass(SortMapper.class);
            job.setReducerClass(SortReducer.class);

            // 指定输入输出文件的路径
            FileInputFormat.setInputPaths(job, new Path(FlowConstant.INPUT_PATH_sort));
            FileOutputFormat.setOutputPath(job, outPath);

            // 设置Mapper输出 K2 V2
            job.setMapOutputKeyClass(FlowBean.class);
            job.setMapOutputValueClass(NullWritable.class);

            // 设置最终输出 K3,V3
            job.setOutputKeyClass(FlowBean.class);
            job.setOutputValueClass(NullWritable.class);

            // 提交执行
            boolean isSuccess = job.waitForCompletion(true);

            System.out.println("流量统计" + (isSuccess ? "成功" : "失败"));
        }catch (Exception ex){
            ex.printStackTrace();
        }

        
    }
    public static class SortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {

        FlowBean k2 = new FlowBean();
        NullWritable v2 = NullWritable.get();
    
        @Override
        protected void map(LongWritable k1, Text v1, Mapper<LongWritable, Text, FlowBean, NullWritable>.Context context)
                throws IOException, InterruptedException {
            String value = v1.toString();
            String[] value_arr = value.split("\t");

            long upload = Long.parseLong(value_arr[1]);
            long download = Long.parseLong(value_arr[2]);
            k2.setTelno(value_arr[0]).setUpFlow(upload).setDownFlow(download);
            context.write(k2, v2);
    
        }
    
    }
    public static class SortReducer extends Reducer<FlowBean,NullWritable,FlowBean,NullWritable> {
        @Override
        protected void reduce(FlowBean k2, Iterable<NullWritable> v2s,
                Reducer<FlowBean, NullWritable, FlowBean, NullWritable>.Context context)
                throws IOException, InterruptedException {
            context.write(k2,NullWritable.get());
            
        }
    
    }
}

