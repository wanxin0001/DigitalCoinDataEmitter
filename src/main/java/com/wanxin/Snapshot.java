package com.wanxin;

import java.util.List;

import com.google.gson.annotations.SerializedName;

public class Snapshot {
	
	@SerializedName("product_id")
	String productId;
	
	String type;
	
	List<List<Double>> bids;
	

	List<List<Double>> asks;


	@Override
	public String toString() {
		return "Snapshot [productId=" + productId + ", type=" + type
				+ ", bids=" + bids.toString() + ", asks=" + asks.toString() + "]";
	}
	
}
