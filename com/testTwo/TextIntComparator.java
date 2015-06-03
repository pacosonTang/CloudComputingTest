package com.testTwo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TextIntComparator extends WritableComparator{

	public TextIntComparator() {
		super(IntPaire.class,true); //注册comparator
		System.out.println(" TextIntComparator 构造方法");
	}

	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		
		System.out.println(" TextIntComparator.compare 方法");
		IntPaire a1 = (IntPaire)a;
		IntPaire b1 = (IntPaire)b;
		if(!a1.getFirstKey().equals(b1.getFirstKey()))
			return a1.getFirstKey().compareTo(b1.getFirstKey());
		else
			return a1.getSecondKey()-b1.getSecondKey();
	}
}
