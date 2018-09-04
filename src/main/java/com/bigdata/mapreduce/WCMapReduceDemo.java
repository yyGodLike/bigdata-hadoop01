package com.bigdata.mapreduce;

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
 * 优化后的MapReduce的Wordcount程序模板编写
 * @author liuhongyang
 *
 */
public class WCMapReduceDemo extends Configured implements Tool{
	
	//step 1:Mapper Class
	public static class MapReduceMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text mapOutputKey = new Text();
		//设置1,出现一次就记录一次
		private IntWritable mapOutputValue = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//读取文件的每一行内容 hadoop mapreduce,转换成String
			String lineValue = value.toString();
			//分个字符串,以空格来分割
			String [] stars = lineValue.split(" ");
			//分割之后将数组中的值一个个取出来，组成<key,value>格式输出 比如<hadoop,1>
			for (String string : stars) {
				mapOutputKey.set(string);
				//通过context上下文输出
				context.write(mapOutputKey, mapOutputValue);
				
				System.out.println("<"+mapOutputKey+","+mapOutputValue+">");
			}
		}
	}
	
	public static class combine extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private IntWritable outputVlue = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
									Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			System.out.println("key="+key);
			
			for (IntWritable value : values) {
				sum += value.get();
				System.out.print(value.get()+" ");
			}
			System.out.println();
			outputVlue.set(sum);
			//输出
			context.write(key, outputVlue);
		}
	}
	
	//step 2:Reduce Class
	public static class MapReduceReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private IntWritable outputVlue = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
									Context context) throws IOException, InterruptedException {
			int sum = 0;
			System.out.println(values.toString());
			for (IntWritable value : values) {
				sum += value.get();
			}
			outputVlue.set(sum);
			//输出
			context.write(key, outputVlue);
		}
	}
	
	public int run(String[]args) throws Exception{
		
		//获取集群中的配置文件信息
		Configuration configuration = this.getConf();
		//创建一个job任务
		Job job = Job.getInstance(configuration, this.getClass().getName());
		//指定MapReduce程序运行的入口或则叫jar包的入口,jar具体运行的哪个类,this.getClass()指的就是当前类
		job.setJarByClass(this.getClass());
		//设置job
		//input输入,输入路径
		Path inPath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPath);
		//output输出,输出路径
		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		//设置Mapper
		job.setMapperClass(MapReduceMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//=======================shuffle start=======================
		//1.分区
		//job.setPartitionerClass(cls);
		
		//2.排序
		//job.setSortComparatorClass(cls);
		
		//3.combine优化
		job.setCombinerClass(combine.class);
		
		//4.分组
		//job.setGroupingComparatorClass(cls);
		
		//=======================shuffle end=======================
		//设置Reduce
		job.setReducerClass(MapReduceReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//提交Job
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0:1;
	}
	
	public static void main(String[] args) throws Exception {
		args = new String[] { "hdfs://bigdata-senior01.liuhongyang.com:8020/user/admin/mapreduce/input",
							"hdfs://bigdata-senior01.liuhongyang.com:8020/user/admin/mapreduce/output4" };
		// run job
		//int status = new WCMapReduceDemo().run(args);
		Configuration configuration = new Configuration();
		int status = ToolRunner.run(configuration, new WCMapReduceDemo(), args);
		// 关闭
		System.exit(status);
	}
}
