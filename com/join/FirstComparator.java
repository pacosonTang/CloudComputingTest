package com.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FirstComparator extends WritableComparator{

	
	public FirstComparator() {
		super(TextPaire.class,true); //注册comparator
	}

	@Override
	@SuppressWarnings("all")
	public int compare(WritableComparable a, WritableComparable b) {
		TextPaire a1 = (TextPaire)a;
		TextPaire b1 = (TextPaire)b;
		return a1.getText().compareTo(b1.getText());
	}
}
