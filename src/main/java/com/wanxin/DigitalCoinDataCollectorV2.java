package com.wanxin;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
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
import com.neovisionaries.ws.client.WebSocketFrame;

/**
 * Hello world!
 *
 */
public class DigitalCoinDataCollectorV2 {	
	
	public static Map<Double, Double> BID_ORDER_BOOK = new HashMap<Double, Double>();
	
	public static Map<Double, Double> ASK_ORDER_BOOK = new HashMap<Double, Double>();
	
	private long totalWasteTime = 0;
	
	private static String namespaceSuffix = "-test";
	
    public static void main( String[] args )
    {
    	DigitalCoinDataCollectorV2 self = new DigitalCoinDataCollectorV2();
    	
    	if (args.length != 0 && args[0].equals("prod")) {
    		namespaceSuffix = "";
    		System.out.println("NamespaceSuffix should be empty for Prod.");
    	}
    	
    	
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
			        	/*
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
			        		
			        		
			        	}
			        	*/
			        	if (messageObj.get("type").getAsString().equals("ticker")) {
			        		if (messageObj.get("time") == null) {
			        			return;
			        		}
			        		//System.out.println(messageObj);
			        		String productId = messageObj.get("product_id").getAsString();
			        		String time = messageObj.get("time").getAsString();
			        		double price = messageObj.get("price").getAsDouble();
			        		double size = messageObj.get("last_size").getAsDouble();
			        		
			        		
			        		
			        		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");//'T'HH:mm:ss.SSSZ
			        		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
			        		Date date = sdf.parse(time.substring(0, time.length() - 8));
			        		Date current = new Date();
			        		String message = String.format("[ProductId=%s][Time=%s][Date=%s][Price=%s][Size=%s][Current=%s]", productId, time, date, price, size, current);
			        		
			        		
			        		System.out.println("Ticker message: " + message);
			        		
			        		emitMetric("Price", price, productId, date, StandardUnit.None);
			        		emitMetric("Size", size, productId, date, StandardUnit.None);
			        		emitMetric("TickerMessageLatency", (current.getTime() - date.getTime()) / 1000, productId, date, StandardUnit.Seconds);			        		
			        		
			        		//reportOrderBook(time, price);
			        	}
			        	
			        	
			        	//Message message = gson.fromJson(messageStr, Message.class);
			        	//if (message.type.equals("done") && !message.reason.equals("canceled")) {
			        		//System.out.println(message);
			        		//System.out.println(messageStr);
			        	//}
			        	
			            // Close the WebSocket connection.
			           // ws.disconnect();
			        	
			        	//Date endTime = new Date();
			        	//totalWasteTime += endTime.getTime() - startTime.getTime();
			        	//System.out.println("Consume time: " + (endTime.getTime() - startTime.getTime()) + " millsecs.");
			        	//System.out.println("Consume time: " + totalWasteTime + " millsecs.");
			        }
			        
			        @Override
			        public void handleCallbackError(WebSocket websocket, Throwable cause) throws Exception {
			        	System.out.println(String.format("[ERROR] Get exception when receiving data. [errMessage=%s]", cause.getMessage()));
			        	cause.printStackTrace();
			        	//System.out.println("Attempt to retry:");
			        	
			        	//subscribeTradeMessages(subscription);
			        }
			        
			        @Override
			        public void onDisconnected(WebSocket websocket,
			            WebSocketFrame serverCloseFrame, WebSocketFrame clientCloseFrame,
			            boolean closedByServer) throws Exception
			        {
			        	System.out.println("Attempt to retry:");
			        	websocket.recreate().connect();
			        }
		
			    })
			    .connect()
			    .sendText(subscription);
		} catch (Exception e) {
			System.out.println(String.format("[ERROR] Get exception when receiving data outside. [errMessage=%s]", e.getMessage()));
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
    	/*
    	emitMetric("BidSum", bidSum);
    	emitMetric("AskSum", askSum);
    	emitMetric("BidCount", BID_ORDER_BOOK.size());
    	emitMetric("AskCount", ASK_ORDER_BOOK.size());
    	emitMetric("Bid-Ask-SumDiff", bidSum - askSum);
    	emitMetric("BidSize", bidSize);
    	emitMetric("AskSize", askSize);
    	emitMetric("Bid-Ask-SizeDiff", bidSize - askSize);
    	emitMetric("Price", price);
    	*/
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
    
    
    private void emitMetric(String metricName, double value, String nameSpace, Date date, StandardUnit unit) {
    	final AmazonCloudWatch cw =
    		    AmazonCloudWatchClientBuilder.defaultClient();
    	

	    MetricDatum datum = new MetricDatum()
	        .withMetricName(metricName)
	        .withUnit(unit)
	        .withValue(value)
	        .withTimestamp(date);
	
	    PutMetricDataRequest request = new PutMetricDataRequest()
	        .withNamespace(nameSpace + namespaceSuffix)
	        .withMetricData(datum);
	
	    PutMetricDataResult response = cw.putMetricData(request);
    	//System.out.println("response:" + response.toString());
    }
    
    private String createSubscription() {
    	JsonArray ja = new JsonArray();
    	ja.add("BTC-USD");
    	ja.add("LTC-USD");
    	ja.add("ETH-USD");
  
        JsonObject jo = new JsonObject();
    	jo.addProperty("type", "subscribe");
    	jo.add("product_ids", ja);
    	
    	JsonArray channels = new JsonArray();
    	//channels.add("heartbeat");
    	channels.add("ticker");
    	//channels.add("level2");
    	jo.add("channels", channels);
    	
    	return jo.toString();
    }
    
    
}
