package net.hubs1.spider.core.command;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.log4j.Logger;

import net.hubs1.spider.core.BaseCommand;
import net.hubs1.spider.core.config.SpiderContext;
import net.hubs1.spider.exception.AccessDeniedException;
import net.hubs1.spider.httpcient.plugin.CustomRedirectStrategy;
import net.hubs1.spider.util.ContentUtil;

public class RequestCommand extends BaseCommand {
	private static ThreadLocal<Map<String,DefaultHttpClient>>   httpClients=new ThreadLocal<Map<String,DefaultHttpClient>>();
	private static Logger log=Logger.getLogger(RequestCommand.class);
	
	private final static int DefaultFailMaxSleepTime=10;//10 minute
	private final static int DefaultIntervalTime=500; //million 
	
	public RequestCommand(int lineNum,String commandName,String[] params,String[] vars){
		super(lineNum, commandName, params,vars);
		this.rsTag="request_command_httpclient";
		SpiderContext.registResource(rsTag, this);
	}
	
	private void initHttpClient(DefaultHttpClient  httpClient){
		HttpConnectionParams.setConnectionTimeout(httpClient.getParams(),1000*60);//连接超时，1分钟  
		HttpConnectionParams.setSoTimeout(httpClient.getParams(), 1000*30);//响应超时，30秒
		
		httpClient.setRedirectStrategy(new CustomRedirectStrategy());
		httpClient.addResponseInterceptor(new HttpResponseInterceptor(){
			 @Override
		    public void process(final HttpResponse response,final HttpContext context) throws HttpException,IOException {  
		        HttpEntity entity = response.getEntity();  
		        Header ceheader = entity.getContentEncoding();  
		        if (ceheader != null) {  
		            HeaderElement[] codecs = ceheader.getElements();  
		            for (int i = 0; i < codecs.length; i++) {  
		                if (codecs[i].getName().equalsIgnoreCase("gzip")) {  
		                    response.setEntity(new GzipDecompressingEntity(response.getEntity()));  
		                    return;  
		                }  
		            }  
		        }
		    }
		});
	}
	
	public List<String> mutiRequest(){
		Object urls=(this.paramStrs!=null&&paramStrs.length==1)?paramStrs[0]:this.getVarData(this.inputVar);
		
		if(urls instanceof String){
			final String url=(String)urls;
			return new ArrayList<String>(){{add(url);}};
		}
		
		if(urls instanceof List){
			List<String> list=(List<String>)urls;
			if(list==null||list.isEmpty()) return new ArrayList<String>(0);
			return list;
		}
		return new ArrayList<String>(0);		
	}
	
	public void get(String[] urls){
		String url=urls!=null&&urls.length==1?(String)urls[0]:null;
		request(url);
	}
	
	public void post(String[] urls){
		String url=urls!=null&&urls.length==1?(String)urls[0]:null;
		request(url);
	}
	
	private DefaultHttpClient getHttpClient(String domain){
		Map<String,DefaultHttpClient> httpClientMap=httpClients.get();
		if(httpClientMap==null){
			httpClientMap=new HashMap<String,DefaultHttpClient>();
			httpClients.set(httpClientMap);
		}
		
		DefaultHttpClient httpClient=httpClientMap.get(domain);

		if(httpClient==null){
			httpClient=new DefaultHttpClient();
			initHttpClient(httpClient);
			httpClientMap.put(domain,httpClient);
		}
		return httpClient;
	}
	
	public boolean request(String url)  {
		
		log.info("execute command:"+commandName+"  the input url is:"+url);
		String domain=(String)getVarData("domain");

		
		if(url==null||url.trim().length()<10){
			 url=(String)this.inputData;
		}
		
		if(url!=null&&url.length()>9){
			setContextVar("preurl", url);
			log.debug(" the preurl is:"+url);
		}
		
		Map<String,String> paramMap=ContentUtil.getRequestParam(this.blockContext);
		
		DefaultHttpClient httpClient=getHttpClient(domain);
		
		
		int repeatNum=0;
		do{
			try{
				requestContent(url,domain,httpClient,commandName.toUpperCase(),paramMap);				
				break;
			}catch(SocketException sc){
				log.error(" SocketException ,request is fail，the Url is:"+url);
				httpClient.getCookieStore().clear();
				httpClient.getConnectionManager().closeExpiredConnections();
			}catch(SocketTimeoutException ste){
				log.error(" SocketTimeoutException,request is fail,the url is:"+url);
				httpClient.getCookieStore().clear();
				httpClient.getConnectionManager().closeExpiredConnections();
			}catch(UnknownHostException uhe){
				log.error(" UnknownHostException,please check dns or releason,the url is  "+url);
			}catch(AccessDeniedException ade){
				log.error(ade);
			}catch(IllegalArgumentException e){
				log.error("url is error,the url is "+url,e);
				break;
			}catch(URISyntaxException e){
				log.error("url is error,the url is "+url,e);
				break;
			}catch(Exception e){
				log.error(" request is fail,that is unkown reseaon : the URL is "+url,e);
			}
			
			if(repeatNum>10) repeatNum=DefaultFailMaxSleepTime;
			if(repeatNum>0){
				log.error(" have error,sleep"+repeatNum+" minute to try again");
				try {Thread.sleep(1000*60*repeatNum);} catch (InterruptedException e1) {}//失败第一次等待0分钟后重试，
			}
			
			String newUrl=checkUrlDomain(url, domain);
			if(newUrl!=null){
				url=newUrl;
			}
		}while(++repeatNum<15);
		return true;
	}
	
	
	/**
	 * 
	 * @param url
	 * @param domain
	 * @return
	 */
	private String checkUrlDomain(String url,String domain){
		if(url.startsWith("http://")){
			int index=url.indexOf("/",8);
			String lsDomani=url.substring(0,index);
			if(!domain.equalsIgnoreCase(lsDomani)){
				String newUrl=domain+url.substring(index);
				log.warn(" change url domain,the old url is:"+url+" the new url is:"+newUrl);
				setContextVar("preurl", newUrl);
				return newUrl;
			}else{
				return null;
			}
		}
		return null;
	}
	
	public  void requestContent(String url,String domain,HttpClient httpClient,String type,Map<String,String> paramMap) throws Exception{
		
		HttpRequestBase request=createRequest(url, domain, type, paramMap);
		String charset=(String)this.getVarData("charset");
		try{
			
			HttpResponse response = httpClient.execute(request);			
			switch(response.getStatusLine().getStatusCode()){
				case HttpStatus.SC_MOVED_PERMANENTLY:
				case HttpStatus.SC_MOVED_TEMPORARILY:
					String newUrl=response.getFirstHeader("Location").getValue();
					if(newUrl.startsWith("/"))
						newUrl=domain+newUrl;
					request.abort();
					if(newUrl.contains("error")){
						log.error(" find error page url is:"+newUrl+" base url is:"+url);
					}
					requestContent(newUrl, domain, httpClient, "GET",null);
					break;
				case HttpStatus.SC_OK:
					HttpEntity entity=response.getEntity();
					byte[] r= EntityUtils.toByteArray(entity);
					EntityUtils.consume(entity);
					
					Header ct=response.getFirstHeader("Content-Type");
					
					if(charset==null){
						charset=ct.getValue().indexOf("=")>-1?ct.getValue().substring(ct.getValue().indexOf("=")+1).toLowerCase():null;
					}
					if(charset!=null&&"gb2312".equals(charset))charset="gbk";//gbk字符集已包含gb2312字符集
					
					if(ct!=null&&ct.getValue()!=null&&ct.getValue().toLowerCase().contains("text")){
						if(charset!=null){
							putResult2Context(new String(r,charset));
						}else{
							putResult2Context(new String(r));//默认utf-8
						}
					}else{
						putResult2Context(r);
					}
					
//					if(url!=null&&url.length()>9) setContextVar("preurl", url);
//					log.debug(" the preurl is:"+url);
					
					/*防止抓取过快*/
					
					int lit=-1;
					String intervalTimeStr=(String)this.getVarData("intervaltime");
					if(intervalTimeStr!=null&&intervalTimeStr.matches("\\d+")){
						lit=Integer.parseInt(intervalTimeStr);
					}
					Thread.sleep(lit>0?lit:DefaultIntervalTime);
					
					break;
				case HttpStatus.SC_NO_CONTENT:
				case HttpStatus.SC_NOT_FOUND:
					putResult2Context(null);//如果访问出错，则情况当前结果
					break;
				case HttpStatus.SC_INTERNAL_SERVER_ERROR:
				default:
					log.error(" have error,the url is "+url+",the status is:"+response.getStatusLine().getStatusCode());
					throw new AccessDeniedException(" vistor page is error,respose status code is:"+response.getStatusLine().getStatusCode());
					//putResult2Context(null);//如果访问出错，则情况当前结果
					//return null;
			}
		}finally{
			request.abort();
		}
	}
	
	/**
	 * @param url
	 * @param domain
	 * @param type
	 * @param paramMap
	 * @return
	 * @throws ParseException
	 * @throws IOException
	 */
	private HttpRequestBase createRequest(String url,String domain,String type,Map<String,String> paramMap) throws ParseException, IOException{
		
		if(url.startsWith("/")) url=domain+url;
		
		HttpRequestBase request=null;
		List<NameValuePair> params =null;
		
		url=url!=null?url.trim().replaceAll(" ","%20"):null;
		
		if(paramMap!=null&&!paramMap.isEmpty()){
			 params = new ArrayList<NameValuePair>();
			for(Map.Entry<String, String> kv:paramMap.entrySet()){
				params.add(new BasicNameValuePair(kv.getKey(),kv.getValue()));
			}
		}
		
		if("POST".equals(type)){
			request=new HttpPost(url);
			setRequestHead(request);
			if(params!=null&&!params.isEmpty()){
				UrlEncodedFormEntity uf=new UrlEncodedFormEntity(params);
				((HttpPost)request).setEntity(uf);
				log.debug(" the real post url is:"+url+", the post params is:"+EntityUtils.toString(uf));
			}
		}else if("GET".equals(type)){
			//List <NameValuePair> ll=URLEncodedUtils.parse(new URI(url),charset==null?"utf-8":charset);
			
			if(params!=null&&!params.isEmpty()){
				String paramStr= EntityUtils.toString(new UrlEncodedFormEntity(params));
				if(paramStr!=null&&paramStr.length()>0){
					if(url.contains("?")){
							url=url+"&"+paramStr;
					}else{
						url=url+"?"+paramStr;
					}
					
					log.debug(" the real get url is:"+url);
					
					/*通过302，301过来的url不应该放入preurl中。false表示url需要通过参数才能区分不同，目前post方法暂不支持*/
					if("false".equals((String)getVarData("deleteUrlParam"))){
						setContextVar("preurl", url);
					}
				}
			}
			request=new HttpGet(url);
			setRequestHead(request);
		}
		return request;
	}
	
	private  void setRequestHead(HttpRequestBase method){
		method.addHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
		method.addHeader("Accept-Encoding", "gzip,deflate");
		method.addHeader("Accept-Language", "zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3");
		method.addHeader("Cache-Control", "max-age=0");
		method.addHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0");
		HashMap<String,String> requestHead=(HashMap<String,String>)this.blockContext.get("RequestHead");
		if(requestHead!=null&&!requestHead.isEmpty()){
			for(Map.Entry<String, String> entry:requestHead.entrySet()){
				method.addHeader(entry.getKey(),entry.getValue());
			}
		}
	}
	
	@Override
	public void destoryResource(){
		Map<String,DefaultHttpClient> map=httpClients.get();
		if(map!=null){
			for(DefaultHttpClient dc:map.values()){
				dc.getConnectionManager().shutdown();
			}
			httpClients.set(null);
		}
	}
	
}
