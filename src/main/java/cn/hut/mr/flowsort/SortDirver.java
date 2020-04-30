package cn.hut.hdfs_demo.mr.flowsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortDirver {
    private static String input = "C:\\Users\\Abuilex\\Documents\\mapreduce\\output";
    private static String output = "C:\\Users\\Abuilex\\Documents\\mapreduce\\sort_output";

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            job.setJarByClass(SortDirver.class);
            job.setMapperClass(SortMapper.class);
            job.setReducerClass(SortReducer.class);

            job.setMapOutputKeyClass(FlowBean.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(FlowBean.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.setInputPaths(job, new Path(input));
            Path outPath = new Path(output);
            FileSystem fs = FileSystem.get(conf);
            //若存在输出路径，则先删除
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);

            System.out.println("流量统计" + (job.waitForCompletion(true) ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
