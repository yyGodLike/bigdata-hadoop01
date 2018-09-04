package com.bigdata.mrSort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组操作是在MapReduce的第四步阶段，根据key分组
 * @author liuhongyang
 *
 */
public class FirstCrouingComparator implements RawComparator<PairWritable> {
	
	/**
	 * 对象比较
	 */
	public int compare(PairWritable o1, PairWritable o2) {
		return o1.getFirst().compareTo(o2.getFirst());
	}

	/**
	 * 数组字节值比较
	 * @param arg0 表示第一个参与比较的字节数组
	 * @param arg1 表示第一个参与比较的字节数组的起始位置
	 * @param arg2 表示第一个参与比较的字节数组的偏移量
	 * @param arg3 表示第二个参与比较的字节数组
	 * @param arg4 表示第二个参与比较的字节数组的起始位置
	 * @param arg5 表示第二个参与比较的字节数组的偏移量
	 */
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return WritableComparator.compareBytes(b1, 0, l1 - 4, b2, 0, l2 - 4);
	}

}
