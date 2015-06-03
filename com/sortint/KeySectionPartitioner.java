package com.sortint;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 按照key的大小来划分区间，当然，key 是 int值
 * 
 * @author zhangdonghao
 * 
 */
public class KeySectionPartitioner<K, V> extends Partitioner<K, V> {

	public KeySectionPartitioner() {
		System.out.println("这是 KeySectionPartitioner 构造方法");
	}

	@Override
	public int getPartition(K key, V value, int numReduceTasks) {
		
		System.out.println("getPartition 方法");
		/**
		 * int值的hashcode还是自己本身的数值
		 */
		// 这里我认为大于maxValue的就应该在第一个分区
		int maxValue = 50;
		int keySection = 0;
		// 只有传过来的key值大于maxValue 并且numReduceTasks比如大于1个才需要分区，否则直接返回0
		if (numReduceTasks > 1 && key.hashCode() < maxValue) {
			int sectionValue = maxValue / (numReduceTasks - 1);
			int count = 0;
			while ((key.hashCode() - sectionValue * count) > sectionValue) {
				count++;
			}
			keySection = numReduceTasks - 1 - count;
		}
		return keySection;
	}

}