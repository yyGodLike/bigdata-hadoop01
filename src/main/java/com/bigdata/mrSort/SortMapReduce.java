package com.bigdata.mrSort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * 二次排序
 * 测试自定义类型key，分组
 * 测试数据	输出结果数据
 * a 1		a 1
 * a 4		a 2
 * b 3		a 4
 * b 1		b 1	
 * a 2		b 3
 * c 1		c 1
 * 
 * @author liuhongyang
 *
 */
public class SortMapReduce extends Configured implements Tool {

	// step 1: Mapper Class
	public static class SortMapper extends Mapper<LongWritable, Text, PairWritable, IntWritable> {

		private PairWritable mapOutputKey = new PairWritable();
		// 出现一次就记录一次
		private IntWritable mapOutputValue = new IntWritable();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// 读取文件的每一行，将Text类型转换成String类型
			String lineValue = value.toString();

			// 分割单词，以空格分割
			String[] strs = lineValue.split(",");

			if (2 != strs.length) {
				return;
			}

			// 设置key输出
			mapOutputKey.set(strs[0], Integer.valueOf(strs[1]));
			mapOutputValue.set(Integer.valueOf(strs[1]));

			// map输出
			context.write(mapOutputKey, mapOutputValue);

		}
	}

	// step2: Reducer Class
	public static class SortReducer extends Reducer<PairWritable, IntWritable, Text, IntWritable> {

		private Text outputKey = new Text();

		@Override
		protected void reduce(PairWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			for (IntWritable value : values) {
				outputKey.set(key.getFirst());

				context.write(outputKey, value);
			}
		}
	}

	/**
	 * Execute the command with the given arguments.
	 * 
	 * @param args
	 *            command specific arguments.
	 * @return exit code.
	 * @throws Exception
	 *             int run(String [] args) throws Exception;
	 */
	// step3: Driver
	public int run(String[] args) throws Exception {

		// 获取集群中的相关配置信息
		Configuration configuration = this.getConf();

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
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(PairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		// ============shuffle===============
		// 1.分区
		job.setPartitionerClass(FirstPartitioner.class);

		// 2.排序
		// job.setSortComparatorClass(cls);

		// 3.优化
		// job.setCombinerClass(WCCombin.class);

		// 4.分组
		job.setGroupingComparatorClass(FirstCrouingComparator.class);
		// ============shuffle===============

		// 设置Reducer
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(2);

		// 提交Job -》 YARN
		boolean isSuccess = job.waitForCompletion(true);

		return isSuccess ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {

		Configuration configuration = new Configuration();

		args = new String[] { "hdfs://bigdata-senior01.ibeifeng.com:8020/sort/input",
				"hdfs://bigdata-senior01.ibeifeng.com:8020/sort/output" };

		// run job
		int status = ToolRunner.run(configuration, new SortMapReduce(), args);
		// exit program
		System.exit(status);
	}

}
