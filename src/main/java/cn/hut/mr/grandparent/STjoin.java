package cn.hut.mr.grandparent;

import java.io.File;
import java.io.IOException;
import java.util.*;
import cn.hut.mr.utils.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class STjoin {
    public static int time = 0;

    // map将输入分割成child和parent，然后正序输出一次作为右表，反//序输出一次作为左表，需要注意的是在输出的value中必须加上左右表//区别标志
    public static class Map extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String childname = new String();
            String parentname = new String();
            String relationtype = new String();
            String line = value.toString();

            String[] values = line.split(" ");
            // Tom Lucy
            // Tom Jack
            // Jone Lucy
            // Jone Jack
            // Lucy Mary
            // Lucy Ben
            // Jack Alice
            // Jack Jesse
            // Terry Alice
            // Terry Jesse
            // Philip Terry
            // Philip Alma
            // Mark Terry
            // Mark Alma
            if (values[0].compareTo("child") != 0) {
                childname = values[0];// Tom
                parentname = values[1];// Lucy
                relationtype = "1"; // 左右表区分标志
                context.write(new Text(values[1]), new Text(relationtype + "-" + childname + "-" + parentname));
                // Lucy,1-Tom-Lucy
                // Jack,1-Tom-Jack
                // Lucy,1-Jone-Lucy
                // Jack,1-Jone-Jack
                // Mary,1-Lucy-Mary
                // Ben,1-Lucy-Ben
                // Alice,1-Jack-Alice
                // Jesse,1-Jack-Jesse
                // Alice,1-Terry-Alice
                // Jesse,1-Terry-Jesse
                // Terry,1-Philip-Terry
                // Alma,1-Philip-Alma
                // Terry,1-Mark-Terry
                // Alma,1-Mark-Alma
                // 左表
                relationtype = "2";
                context.write(new Text(values[0]), new Text(relationtype + "-" + childname + "-" + parentname));
                // Tom,2-Tom-Lucy
                // Tom,2-Tom-Jack
                // Jone,2-Jone-Lucy
                // Jone,2-Jone-Jack
                // Lucy,2-Lucy-Mary
                // Lucy,2-Lucy-Ben
                // Jack,2-Jack-Alice
                // Jack,2-Jack-Jesse
                // Terry,2-Terry-Alice
                // Terry,2-Terry-Jesse
                // Philip,2-Philip-Terry
                // Philip,2-Philip-Alma
                // Mark,2-Mark-Terry
                // Mark,2-Mark-Alma
                // 右表
            }
        }
    }
    // --start --shuffle --
    // Lucy,1-Tom-Lucy
    // Lucy,1-Jone-Lucy
    // Lucy,2-Lucy-Mary
    // Lucy,2-Lucy-Ben
    // Lucy,{1-Tom-Lucy,1-Jone-Lucy,2-Lucy-Mary,2-Lucy-Ben}
    // --end shuffle--

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            if (time == 0) { // 输出表头
                context.write(new Text("grandchild"), new Text("grandparent"));
                time++;
            }
            int grandchildnum = 0;
            String grandchild[] = new String[20];
            int grandparentnum = 0;
            String grandparent[] = new String[20];
            Iterator ite = values.iterator();
            while (ite.hasNext()) {
                String record = ite.next().toString(); // 1-Tom-Lucy
                String[] record_arr = record.split("-");

                String relationtype = record_arr[0];
                String childname = record_arr[1];
                String parentname = record_arr[2];

                // relationtype=1
                // childname=Tom
                // paretname=Lucy

                // 左表，取出child放入grandchild
                if (relationtype.equals("1")) {
                    grandchild[grandchildnum] = childname; // grandchild[0]=Tom, grandchild[1]=Jone
                    grandchildnum++;
                } else {// 右表，取出parent放入grandparent
                    grandparent[grandparentnum] = parentname;// grandparent[0]=Mary,grandparent[1]=Ben
                    grandparentnum++;
                }
            }
            // grandchild和grandparent数组求笛卡儿积
            if (grandparentnum != 0 && grandchildnum != 0) {
                for (int m = 0; m < grandchildnum; m++) {
                    for (int n = 0; n < grandparentnum; n++) {
                        context.write(new Text(grandchild[m]), new Text(grandparent[n])); // 输出结果
                    }
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String argument0 = "D:\\centos\\inputJoin";
        String argument1 = "D:\\centos\\outputJoin";

        if (args.length == 2) {
            argument0 = args[0];
            argument1 = args[1];
        }

        File file = new File(argument1);
        if (file.exists()) {
            // file.delete();
            FileUtils.delFile(file);
        }

        Job job = new Job(conf, "single table join");
        job.setJarByClass(STjoin.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(argument0));
        FileOutputFormat.setOutputPath(job, new Path(argument1));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
