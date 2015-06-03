package com.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextIntComparator extends WritableComparator{

	
	public TextIntComparator() {
		super(TextPaire.class,true); //注册comparator
	}

	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		
		System.out.println("Grouping: FirstTextIntComparator.compare()方法" + "a = " + a.toString() + ", b = " + b.toString());
		TextPaire a1 = (TextPaire)a;
		TextPaire b1 = (TextPaire)b;
		if(!a1.getProductId().equals(b1.getProductId()))
			return a1.getProductId().compareTo(b1.getProductId());
		else
			return a1.getTradeId().compareTo(b1.getTradeId());
	}
}
