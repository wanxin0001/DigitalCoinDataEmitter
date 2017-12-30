package com.wanxin;

import java.util.Date;

public class Message {
	
	String type;
	String side;
	String order_id;
	String reason;
	String product_id;
	String price;
	String remaining_size;
	long sequence;
	Date time;
	
	@Override
	public String toString() {
		return "Message [type=" + type + ", side=" + side + ", order_id="
				+ order_id + ", reason=" + reason + ", product_id="
				+ product_id + ", price=" + price + ", remaining_size="
				+ remaining_size + ", sequence=" + sequence + ", time=" + time
				+ "]";
	}
	
	
}
