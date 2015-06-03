package com.testTwo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextComparator extends WritableComparator{

	public TextComparator() {
		super(IntPaire.class,true); //注册comparator
		System.out.println(" TextComparator 构造方法");
	}

	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		IntPaire a1 = (IntPaire)a;
		IntPaire b1 = (IntPaire)b;
		return a1.getFirstKey().compareTo(b1.getFirstKey());
	}
}
