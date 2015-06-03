package com.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator{

	public GroupComparator() {
		super(TextPaire.class,true); //注册comparator
	}

	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		TextPaire a1 = (TextPaire)a;
		TextPaire b1 = (TextPaire)b;
		System.out.println("Grouping: GroupingComparator.compare()方法" + "a = " + a1.getProductId() + ", b = " + b1.getProductId());
		return -a1.getProductId().compareTo(b1.getProductId());
	}
}
