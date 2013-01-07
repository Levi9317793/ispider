package net.hubs1.bijia.extract;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

public class ExtractCtripHotel {
	
	private static final Log logger = LogFactory.getLog(ExtractCtripHotel.class);
	public static String toLowNum(String num){
		char[] numArray = num.toCharArray();
		for(int i=0;i<numArray.length;i++){
			if(numArray[i]=='一')
				numArray[i]='1';
			else if(numArray[i]=='二')
				numArray[i]='2';
			else if(numArray[i]=='三')
				numArray[i]='3';
			else if(numArray[i]=='四')
				numArray[i]='4';
			else if(numArray[i]=='五')
				numArray[i]='5';
			else if(numArray[i]=='六')
				numArray[i]='6';
			else if(numArray[i]=='七')
				numArray[i]='7';
		}
		return new String(numArray);
	}
	
    public static Map<String, String> extraHotel(String input, String url) {
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        Document doc = Jsoup.parse(input);
        Map<String, String> map = new HashMap<String, String>();
        map.put("source", "ctrip");
        // 国家
        map.put("country", "中国");
        // url
        map.put("source_url", url);
        int urlStartIndex = url.lastIndexOf("/");
        int urlEndIndex = url.lastIndexOf(".");
        if (urlStartIndex >= 0 && urlEndIndex > urlStartIndex)
            map.put("source_id", url.substring(urlStartIndex + 1, urlEndIndex));
        // 名称
        Element nameElement = doc.select("h3#ctl00_MainContentPlaceHolder_h3id").first();
        if (nameElement != null) {
            map.put("name", nameElement.ownText());
            // 英文名称
            Element nameEnElement = nameElement.select("span.en_n").first();
            if (nameEnElement != null)
                map.put("name_en", nameEnElement.ownText());
        }
        // 星级
        Pattern starPattern = Pattern.compile("ctl00_MainContentPlaceHolder_imgStar.*class"); 
        Matcher starMatcher = starPattern.matcher(input); 
        if(starMatcher.find())
        {
        	String starString = starMatcher.group();
        	starMatcher=Pattern.compile(".{1}星级").matcher(starString);
        	if(starMatcher.find()){
        		String starRating = toLowNum(starMatcher.group());
        		map.put("star_rating", starRating.replaceAll("\\D", ""));
        	}
        	//准星级
        	starMatcher=Pattern.compile("（.*）").matcher(starString);
        	if(starMatcher.find()){
        		String diamondString = starMatcher.group();
        		diamondString = diamondString.substring(1,diamondString.length()-1);
        		map.put("diamond_rating", diamondString.replaceAll("[^0-9.]", ""));
        	}
        }
        // 地址
        Element addrElement = doc.select("div.hotel_address").first();
        if (addrElement != null) {
            map.put("address", addrElement.text().replace("查看地图", ""));
            // 城市
            Element cityElement = addrElement.select("span#ctl00_MainContentPlaceHolder_lnkCity").first();
            if (cityElement != null)
                map.put("city", cityElement.text());
            // 行政区
            Element regionElement = addrElement.select("span#ctl00_MainContentPlaceHolder_lnkLocation").first();
            if (regionElement != null)
                map.put("district", regionElement.text());
            // 商圈
            Element businessElement = addrElement.select("a#ctl00_MainContentPlaceHolder_lnkMapZone").first();
            if (businessElement != null)
                map.put("trade_area", businessElement.text());
        }
        // 价格范围
        Element priceElement = doc.select("span#detail_price").first();
        if (priceElement != null)
            map.put("price_range", priceElement.text());
        // 交通信息
        Element trafficElement = doc.select("div#tabContent0").first();
        if (trafficElement != null) {
            StringBuilder trafficBuilder = new StringBuilder();
            Elements trafficElements = trafficElement.select("p");
            for (Element subElement : trafficElements) {
                trafficBuilder.append(subElement.text());
                trafficBuilder.append(",");
            }
            map.put("traffic", trafficBuilder.toString());
        }
        // 酒店简介模块
        Elements elements = doc.select("div.detail2_intro");
        for (Element introElement : elements) {
            Element titleElement = introElement.select("h3").first();
            if (titleElement != null && titleElement.text().equals("酒店简介")) {
                // 简介
                Element desElement = introElement.select("span#ctl00_MainContentPlaceHolder_lbDesc").first();
                if (desElement != null)
                    map.put("description", desElement.text());
                // 开业日期
                Element dateElement = introElement.select("span#ctl00_MainContentPlaceHolder_lbOpenYear").first();
                if (dateElement != null)
                    map.put("open_date", dateElement.text().replaceAll("\\D", ""));
                // 装修时间
                Element decElement = introElement.select("span#ctl00_MainContentPlaceHolder_lbFitmentYear").first();
                if (decElement != null) {
                    map.put("decoration_date", decElement.text().replaceAll("\\D", ""));
                    // 房间总数
                    Node roomNode = decElement.nextSibling();
                    if (roomNode != null) {
                        String roomString[] = roomNode.toString().split(";");
                        if (roomString.length > 1)
                            map.put("total_rooms", roomString[1]);
                    }
                    // 电话
                    Element phoneElement = decElement.nextElementSibling();
                    Pattern phonePattern = Pattern.compile("[-0-9－]+");
                    if (phoneElement != null) {
                        Matcher phonePatcher = phonePattern.matcher(phoneElement.text().replace(" ", ""));
                        if (phonePatcher.find()) {
                            map.put("phone", phonePatcher.group().replace("－", "-"));
                        }
                    }
                    // 传真
                    Element faxElement = phoneElement.nextElementSibling();
                    if (faxElement != null) {
                        Matcher faxPatcher = phonePattern.matcher(faxElement.text());
                        if (faxPatcher.find()) {
                            map.put("fax", faxPatcher.group());
                        }
                    }
                }
                Elements tableElements = introElement.select("tr");
                for (Element tableElement : tableElements) {
                    // 酒店服务
                    if (tableElement.children().size() > 1) {
                        Element keyElement = tableElement.child(0);
                        Element valueElement = tableElement.child(1);
                        if (keyElement != null && keyElement.text().contains("酒店服务")) {
                            if (valueElement != null)
                                map.put("prop_amenities", valueElement.text());
                        }
                        // 客房设施
                        else if (keyElement != null && keyElement.text().contains("客房设施")) {
                            if (valueElement != null)
                                map.put("room_amenities", valueElement.text());
                        }
                        // 信用卡
                        else if (keyElement != null && keyElement.text().contains("接受信用卡")) {
                            StringBuilder stringBuilder = new StringBuilder();
                            for (Element cardElement : valueElement.select("span")) {
                                stringBuilder.append(cardElement.attr("title"));
                                stringBuilder.append(",");
                            }
                            map.put("pay_card", stringBuilder.toString());
                        }
                    }
                }
                break;
            }
        }
        // 图片url
        JSONArray jsonArray = new JSONArray();
        JSONObject json = null;
        String imgUrl = null;
        Element imgElement = doc.select("ul#picList").first();
        try {
            if (imgElement != null) {
                for (Element liElement : imgElement.children()) {
                    Element aElement = liElement.select("a.slide_pic_item").first();
                    if (aElement != null) {
                        json = new JSONObject();
                    	Element aImgElement = aElement.select("img").first();
						if(aImgElement!=null && aImgElement.hasAttr("_src")){
	                        imgUrl = aImgElement.attr("_src");
	                        json.put("url", imgUrl);
	                        json.put("type", imgUrl.substring(imgUrl.lastIndexOf(".") + 1, imgUrl.length()));
	                        if (aElement.select("span").first() != null)
	                            json.put("desc", aElement.select("span").first().text());
	                        jsonArray.put(json);
						}
                    }
                }
                map.put("images", jsonArray.toString());
            }
        } catch (Exception e) {
        	logger.error(e);
        }
        // 精度纬度
        Element geoElement = doc.select("input#hotelCoordinate").first();
        if (geoElement != null) {
            String geoString = geoElement.attr("value");
            String[] geo = geoString.split("\\|");
            if (geo.length > 1) {
                map.put("longitude", geo[0]);
                map.put("latitude", geo[1]);
                map.put("map_source", "tuba");
            }
        }
        return map;
    }
    
    public static String reverseUrl(String url ,String domain ,String prefix){
    	url = url.replace(".", "/");
    	url = url.substring(prefix.length());
    	url = domain + url+".html";
    	return url;
    }
    public static boolean validate (Map<String, String> map ,String url){

    		if(map.containsKey("source_id")&&
    				map.containsKey("country")&&
    				map.containsKey("city")&&
    				map.containsKey("name")&&
    				map.containsKey("address")){
    			return true;
    		}
    		else{
    			logger.error("=============="+url);
    			return false;
    		}
    	
    }
}
