package cn.hut.mr.friends;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.hut.mr.utils.FileUtils;

public class Friends {
    public static void main(String[] args) {
        String argument0 = "D:\\centos\\inputFriends";
        String argument1 = "D:\\centos\\outputFriends";

        if (args.length == 2) {
            argument0 = args[0];
            argument1 = args[1];
        }

        File file = new File(argument1);
        if (file.exists()) {
            // file.delete();
            FileUtils.delFile(file);
        }

        try {
            Configuration conf = new Configuration();
            conf.set("", "root");
            // step 1: 获取job对象
            Job job = Job.getInstance(conf);

            // step 2: 拼接mapper 和 reducer的处理类型
            job.setMapperClass(FDMapper.class);
            job.setReducerClass(FDReducer.class);

            // step 3: 整个job对象的输入输出
            FileInputFormat.setInputPaths(job, new Path(argument0));
            FileOutputFormat.setOutputPath(job, new Path(argument1));

            // step 4: 指定K2 V2的类型,其实就是指定mapper的输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            // step 5: 指定K3 V3的类型,其实就是指定reducer的输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // step 6: 提交这个job
            boolean succeeded = job.waitForCompletion(true);
            if (succeeded == true)
                System.out.println("the job (friends) completed");
            else
                System.out.println("the job (friends) failed");

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    // A:B,C,D,F,E,O
    // B-C,A
    public static class FDMapper extends Mapper<LongWritable, Text, Text, Text> {
        Text k2 = new Text();
        Text v2 = new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String line = v1.toString(); // A:B,C,D,F,E,O C66 = 6*5/2
            String[] lineArr = line.split(":");
            String user = lineArr[0]; // A
            v2.set(user);
            String friends = lineArr[1]; // B,C,D,F,E,O
            String[] friendsArr = friends.split(",");
            List<String> friendsLst = new ArrayList<String>();
            for (String str : friendsArr) {
                friendsLst.add(str);
            }
            Collections.sort(friendsLst); ///-----------------key code- ---------------- 用来消除歧义
            // B
            // C
            // D
            // E
            // F
            // O
            for(int i=0;i<friendsLst.size();i++){ /// ---------------组合 --------------
                String left = friendsLst.get(i);//B
                for(int j=i+1;j<friendsLst.size();j++){
                    String right = friendsLst.get(j);//C
                    k2.set(left+"-"+right); // B-C
                    context.write(k2,v2);
                }
            }

            // B-C,A
            // B-D,A
            // B-F,A
            // B-E,A
            // B-O,A
            // C-D,A
            // C-F,A
            // C-E,A
            // C-0,A
            // D-F,A
            // D-E,A
            // D-0,A
            // E-F,A
            // F-0,A
            // E-0,A

        }
    }

    public static class FDReducer extends Reducer<Text, Text, Text, Text> {

        Text v3 = new Text();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // E-F,{B|C|D|M}

            StringBuilder sb  = new StringBuilder();
            int count =0;
            for(Text text : v2s){
                sb.append(text.toString()+','); // B,C,D,M,
                count ++;
            }
            // sb.deleteCharAt(sb.length()-1);// B,C,D,M
            sb.append(count);

            v3.set(sb.toString());

            context.write(k2,v3);
        
        }
    }
}
