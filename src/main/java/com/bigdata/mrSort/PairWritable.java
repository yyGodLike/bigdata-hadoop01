package com.bigdata.mrSort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 封装一个自定义类型作为key的新类型
 * @author liuhongyang
 *
 */
public class PairWritable implements WritableComparable<PairWritable> {

	private String first;
	private int second;

	public PairWritable() {

	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + second;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PairWritable other = (PairWritable) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second != other.second)
			return false;
		return true;
	}

	public PairWritable(String first, int second) {
		this.set(first, second);
	}

	public void set(String first, int second) {
		this.setFirst(first);
		this.setSecond(second);
	}

	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}
	
	/**
	 * 写
	 */
	public void write(DataOutput out) throws IOException {
		out.writeUTF(first);
		out.writeInt(second);
	}

	/**
	 * 读
	 * 读的顺序要和写的顺序保持一致
	 */
	public void readFields(DataInput in) throws IOException {
		this.first = in.readUTF();
		this.second = in.readInt();
	}

	/**
	 * map shuffle阶段根据key排序时会自动调用compareTo()方法
	 */
	public int compareTo(PairWritable o) {
		int comp = this.first.compareTo(o.getFirst());
		// 判断，如果两个比较字段不等于0，那么直接返回，不需要比较第二个字段，否则就需要比较第二个参数
		if (0 != comp) {
			return comp;
		}
		return Integer.valueOf(this.second).compareTo(Integer.valueOf(o.getSecond()));
	}

	@Override
	public String toString() {
		return first + "\t" + second;
	}
}
