package cn.hut.mr.flowcount.flowcount;

import java.io.Serializable;
import cn.hut.mr.flowcount.bean.FlowBean;
import cn.hut.mr.flowcount.constants.FlowConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Description 流量统计的测试类
 *
 * @author HZX
 * @date 2020/4/23 10:57
 */
public class Run {

    /**
     * @Description 程序执行的入口
     *
     * @param args
     * @return void
     */

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String argument1 = FlowConstant.INPUT_PATH;
        String argument2 = FlowConstant.OUTPUT_PATH;

        if (args.length == 2) {
            argument1 = args[0];
            argument2 = args[1];
        }
        // 构建Configuration对象
        Configuration conf = new Configuration();
        int tasks = 7;

        // 清理输出文件夹
        Path outPath = new Path(argument2);
        // 判断输出文件是否已经存在，存在的话就将其删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        // 构建一个Job对象
        Job job = Job.getInstance(conf);

        // 设置job所用的类在哪个jar包
        job.setJarByClass(Run.class);
        // 设置Mapper 和 Reducer所在的类
        job.setMapperClass(FCMapper.class);
        job.setReducerClass(FCReducer.class);

        // 指定输入输出文件的路径
        FileInputFormat.setInputPaths(job, new Path(argument1));
        FileOutputFormat.setOutputPath(job, outPath);

        // 设置Mapper输出 K2 V2
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 设置partitioner
        job.setPartitionerClass(FCPartitioner.class);
        job.setNumReduceTasks(tasks);

        // 设置最终输出 K3,V3
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 提交执行
        boolean isSuccess = job.waitForCompletion(true);

        System.out.println("流量统计" + (isSuccess ? "成功" : "失败"));

    }
}
