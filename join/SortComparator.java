package com.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator{

	public SortComparator() {
		super(TextPaire.class,true); //注册comparator
	}

	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		TextPaire a1 = (TextPaire)a;
		TextPaire b1 = (TextPaire)b;
		System.out.println("Sorting: FirstComparator.compare()方法" + "a = " + a.toString() + ", b = " + b.toString());
		if(!a1.getProductId().equals(b1.getProductId()))
            return -a1.getProductId().compareTo(b1.getProductId());
        else
            return -a1.getTradeId().compareTo(b1.getTradeId());
	}
}
