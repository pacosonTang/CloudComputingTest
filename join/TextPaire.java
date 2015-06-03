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

	private String productId;//ID
	private String tradeId; // 标识商品表(0)还算支付表(1)
	
	public TextPaire() {}
	
	public TextPaire(String productId, String tradeId) {
		super();
		this.productId = productId;
		this.tradeId = tradeId;
	}
	
	@Override
	public int compareTo(TextPaire o) { // 1 0 -1
		
		System.out.println("TextPaire.compareTo()方法");
		return o.getProductId().compareTo(this.getProductId());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.productId = in.readUTF();
		this.tradeId= in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.productId);
		out.writeUTF(this.tradeId);
	}

	/*
	 *set and get methods 
	 */
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "productId = " + productId + ", tradeId = " + tradeId;
	}

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public String getTradeId() {
		return tradeId;
	}

	public void setTradeId(String tradeId) {
		this.tradeId = tradeId;
	}
}
