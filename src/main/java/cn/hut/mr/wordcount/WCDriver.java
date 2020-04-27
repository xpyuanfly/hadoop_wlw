package cn.hut.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCDriver {

    public static void main(String[] args) {    
        // String arg1 = args[0];
        // String arg2 = args[1];
        String arg1 = "d://centos//input";
        String arg2 = "d://centos//output";
        try {
            Job job = Job.getInstance(new Configuration());
            // 设置类路径
            job.setJarByClass(WCDriver.class);
            // 设置Mapper和Reducer
            job.setMapperClass(WCMapper.class);
            job.setReducerClass(WCReducer.class);
            //// 设置Mapper和Reducer的输出
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            // 文件的输入输出
            FileInputFormat.setInputPaths(job, new Path(arg1));
            FileOutputFormat.setOutputPath(job, new Path(arg2));
            // 判断job成功与否
            boolean b = job.waitForCompletion(true);
            System.exit(b ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}