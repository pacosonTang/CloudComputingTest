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
		TextPaire a1 = (TextPaire)a;
		TextPaire b1 = (TextPaire)b;
		if(!a1.getText().equals(b1.getText()))
			return a1.getText().compareTo(b1.getText());
		else
			return a1.getId()-b1.getId();
	}
}
