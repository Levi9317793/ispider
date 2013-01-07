package net.hubs1.spider.util;

import java.io.IOException;

import net.hubs1.spider.store.HbaseClient;
import net.hubs1.spider.store.KVBean;
import net.hubs1.spider.store.RecordsBean;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

public class HTK2QuarHotel {

		public static void cat() throws ClientProtocolException, IOException{
			HttpClient httpClient=new DefaultHttpClient();
			HttpGet request=new HttpGet("http://config.hn/api/detailInfo.html?username=HTKXML_QUNAR");
			HttpResponse response=httpClient.execute(request);
			if(response.getStatusLine().getStatusCode()==HttpStatus.SC_OK){
				
				
				HttpEntity entity=response.getEntity();
				byte[] r= EntityUtils.toByteArray(entity);
				EntityUtils.consume(entity);
				
				String jsonStr=new String(r);
				JSONObject jo=JSONObject.fromObject(jsonStr);
				if(jo.getInt("code")==200){
					JSONArray ja=jo.getJSONObject("result").getJSONArray("ratecodeslist");
					
					System.out.println(" size:"+ja.size());
					for(int x=0;x<ja.size();x++){
						jo=ja.getJSONObject(x);
						
						String hotelListStr=null;
						if("0".equals(jo.getString("rostertype"))){
							hotelListStr=jo.getString("whiteHotels");
						}else{
							hotelListStr=jo.getString("hotels");
						}
						
						
						if(hotelListStr==null||hotelListStr.trim().length()<1) return;
						
						HbaseClient hbclient=HbaseClient.getInstance();
						
	//					hbclient.delete(tableName, rowkey)
						
						String[] hotels=hotelListStr.trim().split(",");
						System.out.println(" hotel:"+hotels.length);
						RecordsBean[] rbs=new RecordsBean[hotels.length];
						for(int i=0;i<hotels.length;i++){
							
							RecordsBean rb=hbclient.get("spider_prop", "prop_base", "fog:"+hotels[i], new String[0]);
							if(rb==null) {
								System.out.println(" hotel id:"+hotels[i]);
								continue;
							}
							
							for(KVBean kv:rb.getAttributes()){
								if("source".equals(kv.getKey())) kv.setValue("htk2qunar");
							}
							
							rb.setRowkey(rb.getRowkeyStr().replace("fog", "htk2qunar").getBytes());
							rbs[i]=rb;
						}
						hbclient.writeData("spider_prop", rbs);
					}
				}
			}
		}
		
		public static void main(String[] args) throws ClientProtocolException, IOException {
			HTK2QuarHotel.cat();
		}
}
