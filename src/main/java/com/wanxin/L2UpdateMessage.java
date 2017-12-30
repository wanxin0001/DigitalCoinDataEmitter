package com.wanxin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import com.google.gson.annotations.SerializedName;

public class L2UpdateMessage {
	
	String type;
	
	@SerializedName("product_id")
	String productId;
	
	Date time;
	
	String[][] changes;
	
	String side;
	
	double price;
	
	double size;
	
	public String getSide() {
		return changes[0][0];
	}
	
	public double getPrice() {
		return Double.parseDouble(changes[0][1]);
	}
	
	public double getSize() {
		return Double.parseDouble(changes[0][2]);
	}
	
	public List<Double> getPair() {
		List<Double> result = new ArrayList<Double>();
		result.add(getPrice());
		result.add(getSize());
		return result;
	}
	
	
	@Override
	public String toString() {
		return String.format("L2UpdateMessage [side=%s][price=%s][size=%s]", getSide(), getPrice(), getSize());
	}
}
