package com.bigdata.mapreduce;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 网站指标PV统计,根据省份ID统计访问量
 * @author liuhongyang
 *
 */
public class PvMapReduceDemo extends Configured implements Tool{
	
	/**
	 * map类
	 * @author liuhongyang
	 *
	 */
	public static class MapReduceMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
		
		private IntWritable mapOutputKey = new IntWritable();
		private IntWritable mapOutputValue = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//文件的每一行内容，将text转换成String类型
			String lineValue = value.toString();
			//分割数据，以制表符分割
			String [] values = lineValue.split("\t");
			//判断数据的字段是否小于30个，如果是则直接返回
			if(30 > values.length){
				//自定计数器
				context.getCounter("PV_COUNTER", "LENGTH_LT_30_COUNTER").increment(1L);
				return;
			}
			//判断url地址是否为空
			String urlValue = values[1];
			if(StringUtils.isBlank(urlValue)){
				//自定计数器
				context.getCounter("PV_COUNTER", "URL_BLANK_COUNTER").increment(1L);
				return;
			}
			//获取省份ID,省份ID位于一行内容的24位置，下标从0开始
			String provinceValue = values[23];
			//判断省份ID字段是否为空
			if(StringUtils.isBlank(provinceValue)){
				//自定计数器
				context.getCounter("PV_COUNTER", "PROVINCEVALUE_BLANK_COUNTER").increment(1L);
				return;
			}
			//将省份ID字段转换为int类型
			Integer provinceId = Integer.MAX_VALUE;
			try {
				provinceId = Integer.valueOf(provinceValue);
			} catch (Exception e) {
				//自定计数器
				context.getCounter("PV_COUNTER", "PROVINCEID_VALUEOF_COUNTER").increment(1L);
				return;
			}
			//set IntWritable类型对象值
			mapOutputKey.set(provinceId);
			//map输出
			context.write(mapOutputKey, mapOutputValue);
		}
		
	}
	
	/**
	 * reduce类
	 * @author liuhongyang
	 *
	 */
	public static class MapReduceReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		
		private IntWritable outputValue = new IntWritable();
		
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			outputValue.set(sum);
			context.write(key, outputValue);
		}
		
	}
	
	

	/**
	 * 执行的run方法
	 */
	@Override
	public int run(String[] args) throws Exception {
		
		//获取集群中的相关配置信息
		Configuration configuration = this.getConf();
		//创建一个job任务
		Job job = Job.getInstance(configuration, this.getClass().getName());
		//设置MapReduce程序运行的入口，或者叫做jar的入口，jar具体运行的哪个类
		job.setJarByClass(this.getClass());
		//设置job属性
		//设置input输入路径
		Path inpath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inpath);
		//设置output输出路径
		Path outpath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outpath);
		//设置mapper
		job.setMapperClass(MapReduceMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		//设置reduce
		job.setReducerClass(MapReduceReduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		args = new String[]{"hdfs://bigdata-senior01.liuhongyang.com:8020/user/admin/PV/input",
							"hdfs://bigdata-senior01.liuhongyang.com:8020/user/admin/PV/output1"};
		int status = ToolRunner.run(configuration, new PvMapReduceDemo(),args);
		System.exit(status);
	}
	
}
