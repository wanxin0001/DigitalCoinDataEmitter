package com.wanxin;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketAdapter;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketFactory;

public class DigitalCoinDataCollector {
	
	public static void main( String[] args )
    {
		DigitalCoinDataCollector self = new DigitalCoinDataCollector();
    	
    	
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
			        	
			        	System.out.println(messageStr);
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
