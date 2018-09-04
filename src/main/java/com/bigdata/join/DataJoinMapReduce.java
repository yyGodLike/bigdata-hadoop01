package com.bigdata.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * reduce端join
 * @author liuhongyang
 *
 */
public class DataJoinMapReduce extends Configured implements Tool {

	// step 1: Mapper
	public static class DataJoinMapper extends Mapper<LongWritable, Text, LongWritable, DataJoinWritable> {

		// map output key
		private LongWritable mapOutputKey = new LongWritable();

		// map output value
		private DataJoinWritable mapOutputValue = new DataJoinWritable();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// line value获取每一行的数据
			String lineValue = value.toString();

			// split分割切片，逗号隔开
			String[] vals = lineValue.split(",");

			int length = vals.length;

			// 首先判断数据是否符合，如果不是3也不是4的话直接返回
			if ((3 != length) && (4 != length)) {
				return;
			}

			// get cid 获取ID
			Long cid = Long.valueOf(vals[0]);

			// get name，用户名称&商品名称，name
			String name = vals[1];

			// set customer，数据长度为3的话，获取相关信息
			if (3 == length) {
				String phone = vals[2];

				// set
				mapOutputKey.set(cid);
				mapOutputValue.set("customer", name + "," + phone);
			}

			// set order数据长度为4的话，获取相关信息
			if (4 == length) {
				String price = vals[2];
				String date = vals[3];

				// set
				mapOutputKey.set(cid);
				mapOutputValue.set("order", name + "," + price + "," + date);
			}

			// output
			context.write(mapOutputKey, mapOutputValue);

		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
		}

	}

	// step 2: Reducer
	public static class DataJoinReducer extends Reducer<LongWritable, DataJoinWritable, NullWritable, Text> {

		private Text outputValue = new Text();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		}

		@Override
		protected void reduce(LongWritable key, Iterable<DataJoinWritable> values, Context context)
				throws IOException, InterruptedException {
			// 用户信息为null
			String customerInfo = null;
			// 集合用于存放订单信息
			List<String> orderList = new ArrayList<String>();

			for (DataJoinWritable value : values) {
				if ("customer".equals(value.getTag())) {
					customerInfo = value.getData();
				} else if ("order".equals(value.getTag())) {
					orderList.add(value.getData());
				}
			}

			// output
			for (String order : orderList) {

				// ser outout value信息的拼接组合
				outputValue.set(key.get() + "," + customerInfo + "," + order);

				// output
				context.write(NullWritable.get(), outputValue);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
		}

	}

	/**
	 * Execute the command with the given arguments.
	 * 
	 * @param args
	 *            command specific arguments.
	 * @return exit code.
	 * @throws Exception
	 */
	// int run(String [] args) throws Exception;

	// step 3: Driver
	public int run(String[] args) throws Exception {

		Configuration configuration = this.getConf();

		Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
		job.setJarByClass(DataJoinMapReduce.class);

		// set job
		// input
		Path inpath = new Path(args[0]);
		FileInputFormat.addInputPath(job, inpath);

		// output
		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);

		// Mapper
		job.setMapperClass(DataJoinMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DataJoinWritable.class);

		// ============shuffle=================
		// 1.partitioner
		// job.setPartitionerClass(cls);

		// 2.sort
		// job.setSortComparatorClass(cls);

		// 3.group
		// job.setGroupingComparatorClass(cls);

		// job.setCombinerClass(class);

		// ============shuffle=================

		// Reducer
		job.setReducerClass(DataJoinReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// submit job -> YARN
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {

		Configuration configuration = new Configuration();

		args = new String[] { "hdfs://bigdata-senior01.ibeifeng.com:8020/user/beifeng/join/input",
				"hdfs://bigdata-senior01.ibeifeng.com:8020/user/beifeng/join/output2" };

		// run job
		int status = ToolRunner.run(configuration, new DataJoinMapReduce(), args);

		// exit program
		System.exit(status);
	}
}
