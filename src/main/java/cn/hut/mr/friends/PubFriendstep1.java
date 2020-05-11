import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PubFriendstep1 {
	private static class PubFriendMapper1 extends Mapper<LongWritable, Text, Text, Text> {

		Text k2 = new Text();
		Text v2 = new Text();

		@Override
		protected void map(LongWritable k1, Text v1, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// 1.��ȡһ��
			// A:B,C,D,F,E,O
			// B:A,C,E,K

			String line = v1.toString();
			// 2.����\t�и�
			String[] lineSplit = line.split(":");
			String user = lineSplit[0];
			String[] others = lineSplit[1].split(",");
			// ��userΪkey �����û�idΪvalue
			for (String name : others) {
				k2.set(user);
				v2.set(name);
				// A,B
				// A,C
				// A,D
				// A,F
				// ...
				context.write(k2, v2);
			}
		}
	}

	private static class PubFriendReducer1 extends Reducer<Text, Text, Text, Text> {

		// A,{B,C,D,F,E,O}
		// B,{A,C,E,K}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringBuffer kstr = new StringBuffer();

			// ����
			Vector<String> vals = new Vector<String>();
			for (Text text : values) {
				vals.add(text.toString());
			}
			// vals= B,C,D,F,E,O
			java.util.Collections.sort(vals); //=============================key code =======================
			// vals= B,C,D,E,F,O
			for (String s : vals) {
				kstr.append(s + ",");
			}
			context.write(key, new Text(kstr.toString()));
			// A B,C,D,E,F,O
			// B A,C,E,K 
		}
	}

	public static boolean startStep1(String inPath, String outPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		boolean res = false;
		try {
			// 1.��ȡjob
			Job job = Job.getInstance(new Configuration());
			// 2.����jar file
			job.setJarByClass(PubFriendstep1.class);
			// 3.����map class
			job.setMapperClass(PubFriendMapper1.class);
			// 4.����reducer class
			job.setReducerClass(PubFriendReducer1.class);

			// 5.����map�����
			job.setMapOutputKeyClass(Text.class);
			// 6����reduce �����
			job.setMapOutputValueClass(Text.class);

			// 7.������������·��
			FileInputFormat.setInputPaths(job, new Path(inPath));
			FileOutputFormat.setOutputPath(job, new Path(outPath));
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			// 8.�ύ����
			res = job.waitForCompletion(true);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return res;

	}
}
