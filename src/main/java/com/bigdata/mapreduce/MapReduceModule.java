package com.bigdata.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bigdata.mapreduce.MapReduceModule.MapReduceMapper.MapReduceReducer;

public class MapReduceModule {

	// step 1: Mapper Class
	public static class MapReduceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private Text mapOutputKey = new Text();
		// 出现一次就记录一次
		private IntWritable mapOutputValue = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// 读取文件的每一行，将Text类型转换成String类型
			String lineValue = value.toString();
			// 分割单词，以空格分割
			String[] strs = lineValue.split(" ");

			// 分割之后将单词从数组中一个个拿出来，组成<keyvalue>，比如<hadoop,1>
			for (String str : strs) {
				// 设置key输出
				mapOutputKey.set(str);

				// map输出
				context.write(mapOutputKey, mapOutputValue);
			}
		}

		// step2: Reducer Class
		public static class MapReduceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

			private IntWritable outputValue = new IntWritable();

			@Override
			protected void reduce(Text key, Iterable<IntWritable> values, Context context)
					throws IOException, InterruptedException {

				// temp: sum
				int sum = 0;

				// 对值进行跌代累加
				for (IntWritable value : values) {
					// total
					sum += value.get();
				}

				// set output value
				outputValue.set(sum);

				// 最终输出
				context.write(key, outputValue);
			}
		}

	}

	// step3: Driver
	public int run(String[] args) throws Exception {

		// 获取集群中的相关配置信息
		Configuration configuration = new Configuration();

		// 创建一个Job任务
		Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
		// 整个MapReduce程序运行的入口，或者叫jar包的入口，jar具体运行的是哪个类
		job.setJarByClass(this.getClass());

		// 设置Job
		// input输入，输入路径
		Path inpath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inpath);

		// outout输出，输出路径
		Path outpath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outpath);

		// 设置Mapper
		job.setMapperClass(MapReduceMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 设置Reducer
		job.setReducerClass(MapReduceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 提交Job运行，返回是否运行成功
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {

		args = new String[] { "hdfs://bigdata-senior01.liuhongyang.com:8020/user/admin/mapreduce/input",
				"hdfs://bigdata-senior01.liuhongyang.com:8020/user/admin/mapreduce/output4" };

		// run job
		int status = new MapReduceModule().run(args);

		// 关闭
		System.exit(status);
	}

}
