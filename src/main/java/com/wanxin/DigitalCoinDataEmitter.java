package com.wanxin;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketAdapter;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketFactory;

/**
 * Hello world!
 *
 */
public class DigitalCoinDataEmitter 
{	
	
	public static Map<Double, Double> BID_ORDER_BOOK = new HashMap<Double, Double>();
	
	public static Map<Double, Double> ASK_ORDER_BOOK = new HashMap<Double, Double>();
	
	private long totalWasteTime = 0;
	
    public static void main( String[] args )
    {
    	DigitalCoinDataEmitter self = new DigitalCoinDataEmitter();
    	
    	
    	String subscription = self.createSubscription();
    	self.subscribeTradeMessages(subscription);
    }
    
    private void subscribeTradeMessages(String subscription) {
    	try {
			new WebSocketFactory()
				.createSocket("wss://ws-feed.gdax.com")
			    .addListener(new WebSocketAdapter() {
			        @Override
			        public void onTextMessage(WebSocket ws, String messageStr) throws ParseException {
			        	
			        	
			        	Date startTime = new Date();
			        	Gson gson = new Gson();
			        	JsonParser parser = new JsonParser();
			        	JsonObject messageObj = parser.parse(messageStr).getAsJsonObject();
			        	
			        	//if (true) {
			        	//	if (messageObj.get("type").getAsString().equals("ticker")) {
			        	//		System.out.println("Current time: " + startTime + " " + messageStr);
			        	//	}
			        	if (messageObj.get("type").getAsString().equals("snapshot")) {
			        		Snapshot snapshot = gson.fromJson(messageStr, Snapshot.class);
			        		System.out.println(snapshot.toString());
			        		
			        		buildOrderBook(snapshot);
			        		System.out.println("Complete order book build.");
			        		//reportOrderBook();
			        	} else if (messageObj.get("type").getAsString().equals("l2update")) {
			        		L2UpdateMessage l2Msg = gson.fromJson(messageStr, L2UpdateMessage.class);
			        		if (l2Msg.getSide().equals("buy")) {
			        			updateOrderBook(l2Msg.getPair(), BID_ORDER_BOOK);
			        			//System.out.println("buy done");
			        		} else if (l2Msg.getSide().equals("sell")) {
			        			updateOrderBook(l2Msg.getPair(), ASK_ORDER_BOOK);
			        			//System.out.println("sell done");
			        		}
			        		
			        		
			        	} else if (messageObj.get("type").getAsString().equals("ticker")) {
			        		String time = messageObj.get("time").getAsString();
			        		double price = messageObj.get("price").getAsDouble();
			        		
			        		reportOrderBook(time, price);
			        	}
			        	
			        	
			        	//Message message = gson.fromJson(messageStr, Message.class);
			        	//if (message.type.equals("done") && !message.reason.equals("canceled")) {
			        		//System.out.println(message);
			        		//System.out.println(messageStr);
			        	//}
			        	
			            // Close the WebSocket connection.
			           // ws.disconnect();
			        	
			        	Date endTime = new Date();
			        	totalWasteTime += endTime.getTime() - startTime.getTime();
			        	//System.out.println("Consume time: " + (endTime.getTime() - startTime.getTime()) + " millsecs.");
			        	//System.out.println("Consume time: " + totalWasteTime + " millsecs.");
			        }
			    })
			    .connect()
			    .sendText(subscription);
		} catch (WebSocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void reportOrderBook(String time, double price) throws ParseException {
    	double bidSum = 0;
    	double askSum = 0;
    	double bidSize = 0;
    	double askSize = 0;
    	for (Map.Entry<Double, Double> entry : BID_ORDER_BOOK.entrySet()) {
    		if (Math.abs(entry.getKey() - price) <= 10) {
    			bidSum += (price - entry.getKey()) * entry.getValue();
    			bidSize += entry.getValue();
    		}
    	}
    	
    	for (Map.Entry<Double, Double> entry : ASK_ORDER_BOOK.entrySet()) {
    		if (Math.abs(entry.getKey() - price) <= 10) {
    			askSum += (entry.getKey() - price) * entry.getValue();
    			askSize += entry.getValue();
    		}
    	}
    	
    	Date current = new Date();
    	
    	String message = String.format("Report current order book: [Date=%s]"
    			+ "[BidSum=%.0f][AskSum=%.0f][Diff=%.0f]"
    			+ "[BidSize=%.0f][AskSize=%.0f][Diff=%.0f][messageTime=%s][currentTime=%s]", 
    			time, bidSum, askSum, bidSum - askSum, bidSize, askSize, bidSize - askSize, time, current);
    	
    	emitMetric("BidSum", bidSum);
    	emitMetric("AskSum", askSum);
    	emitMetric("BidCount", BID_ORDER_BOOK.size());
    	emitMetric("AskCount", ASK_ORDER_BOOK.size());
    	emitMetric("Bid-Ask-SumDiff", bidSum - askSum);
    	emitMetric("BidSize", bidSize);
    	emitMetric("AskSize", askSize);
    	emitMetric("Bid-Ask-SizeDiff", bidSize - askSize);
    	emitMetric("Price", price);
    	
    	System.out.println(message);
    }
    
    private void buildOrderBook(Snapshot snapshot) {
    	
    	for (List<Double> item : snapshot.bids) {
    		updateOrderBook(item, BID_ORDER_BOOK);
    	}
    	
    	for (List<Double> item : snapshot.asks) {
    		updateOrderBook(item, ASK_ORDER_BOOK);
    	}
    }
    
    private void updateOrderBook(List<Double> item, Map<Double, Double> orderBook) {
    	
		double price = item.get(0);
		double size = item.get(1);
		
		if (size == 0) {
			orderBook.remove(price);
		} else {
			orderBook.put(price, size);
		}
    }
    
    private void emitMetric(String metricName, double value) {
    	final AmazonCloudWatch cw =
    		    AmazonCloudWatchClientBuilder.defaultClient();
    	

	    MetricDatum datum = new MetricDatum()
	        .withMetricName(metricName)
	        .withUnit(StandardUnit.None)
	        .withValue(value);
	
	    PutMetricDataRequest request = new PutMetricDataRequest()
	        .withNamespace("Bitcoin-USD")
	        .withMetricData(datum);
	
	    PutMetricDataResult response = cw.putMetricData(request);
    	//System.out.println("response:" + response.toString());
    }
    
    private String createSubscription() {
    	JsonArray ja = new JsonArray();
    	ja.add("BTC-USD");
  
        JsonObject jo = new JsonObject();
    	jo.addProperty("type", "subscribe");
    	jo.add("product_ids", ja);
    	
    	JsonArray channels = new JsonArray();
    	//channels.add("heartbeat");
    	channels.add("ticker");
    	channels.add("level2");
    	jo.add("channels", channels);
    	
    	return jo.toString();
    }
    
    
}
