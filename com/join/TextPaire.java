package com.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * 自定义类型
 * 
 * @author Tang Rong (Pacoson Tang)
 *
 */
public class TextPaire implements WritableComparable<TextPaire>{

	private String text;//ID
	private int id; // 标识商品表(0)还算支付表(1)
	
	@Override
	public int compareTo(TextPaire o) {
		// TODO Auto-generated method stub
		return o.getText().compareTo(this.getText()); //1 0 -1
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.text = in.readUTF();
		this.id= in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(text);
		out.writeInt(id);
	}

	/*
	 *set and get methods 
	 */
	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
}
