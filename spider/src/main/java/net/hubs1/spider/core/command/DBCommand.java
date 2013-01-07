package net.hubs1.spider.core.command;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import net.hubs1.spider.core.BaseCommand;
import net.hubs1.spider.core.DBClient;
import net.hubs1.spider.store.DBFactory;
import net.hubs1.spider.store.KVBean;
import net.hubs1.spider.store.RecordsBean;
import net.hubs1.spider.util.ContentUtil;
import net.hubs1.spider.util.SpiderUtil;

public class DBCommand extends BaseCommand{
	public DBCommand(int lineNum, String commandName, String[] params,String[] vars) {
		super(lineNum, commandName, params,vars);
		db=DBFactory.getDBClient();
	}
	private  DBClient db=null;
	private static Logger log=Logger.getLogger(DBCommand.class);
	
	public void saveurl(String...params){
		Object urlStr=this.inputData;//content.get(CURRENT);
		
		String field=params!=null&&params.length>0?params[0]:"url";
		String family=params!=null&&params.length>1?params[1]:"base";
		String table=params!=null&&params.length>2?params[2]:"download_url";
		
		String[] urls=null;
		if(urlStr==null){
			log.info(" the url is null");
			return;
		}
		if(urlStr instanceof String){
			String ustr=(String)urlStr;
			urls=ustr!=null?ustr.split(","):new String[0];
		}else{
			if(urlStr==null) return ;
			if(urlStr instanceof List){
				
				Collection<String> list=(Collection<String>)urlStr;
				urls=list.toArray(new String[list.size()]);
			}
		}
		
		if(urls!=null&&urls.length>0){
			
			String rowkeyPrefix=(String)getVarData("rowkeyPrefix");
			boolean deleteUrlParam="false".equals((String)getVarData("deleteUrlParam"))?false:true;
			
			if(rowkeyPrefix==null) rowkeyPrefix="";
			String  source=(String)getVarData("source");
			RecordsBean[] rbs=new RecordsBean[urls.length];
			for(int i=0;i<urls.length;i++){
				String url=urls[i];
				String rowkey=SpiderUtil.url2RowKey(url,rowkeyPrefix,deleteUrlParam,deleteUrlParam);
				
				RecordsBean rb=null;
				if("url".equals(field))
					rb=new RecordsBean(family,rowkey,new KVBean("source",source),new KVBean(field,url));
				else
					rb=new RecordsBean(family,rowkey,new KVBean(field,url));
				rbs[i]=rb;
			}
			boolean isOK=db.writeData(table,rbs);
			log.info("save url to table "+table+"."+family+"."+"field"+" num is："+urls.length+(isOK?" success":" fail"));
		}
	}
	
	public void scanurl(String... params){
		String start=params!=null&&params.length>0?params[0]:null;
		String end=params!=null&&params.length>1?params[1]:null;
		
		String field=params!=null&&params.length>2?params[2]:"url";
		String family=params!=null&&params.length>3?params[3]:"base";
		String table=params!=null&&params.length>4?params[4]:"download_url";
		
		List<KVBean> urls= db.scan(table, family, start, end, field);
		log.info("scan url from "+table+"."+family+"."+field+"number is:"+urls.size());
		this.putResult2Context(urls);
	}
	
	
	public void savepage(String...params){
		Object  po=this.inputData;
		byte[] page=null;
		String url=(String)getVarData("preurl");
		
		
		String field=params!=null&&params.length>0?params[0]:"page";
		String family=params!=null&&params.length>1?params[1]:"base";
		String table=params!=null&&params.length>2?params[2]:"download_page";
		
		if(po==null||url==null){
			log.error(" page content or url is null:");
			return;
		}
		
		if(po instanceof String){
			page=((String)po).getBytes();
		}else{
			page=(byte[])po;
		}
		
		String  source=(String)getVarData("source");
		String rowkeyPrefix=(String)getVarData("rowkeyPrefix");
		boolean deleteUrlParam="false".equals((String)getVarData("deleteUrlParam"))?false:true;
		String rowkey=SpiderUtil.url2RowKey(url,rowkeyPrefix,deleteUrlParam,deleteUrlParam);
		
		RecordsBean rb=null;
		if("page".equals(field))
			rb=new RecordsBean(family,rowkey,new KVBean("source",source),new KVBean(field,page));
		else
			rb=new RecordsBean(family,rowkey,new KVBean(field,page));
		
		boolean isOK=db.writeData(table,new RecordsBean[]{rb});
		log.info("save  page to table "+table+"."+family+"."+field+" ["+rowkey+"] is "+(isOK?"success":"fail"));
	}
	
	public void saveimage(String...params){
		if(this.inputData==null){
			log.error(" the data is null ");
			return;
		}
		byte[] image=(byte[])this.inputData;
		
		String field=params!=null&&params.length>0?params[0]:"image";
		String family=params!=null&&params.length>1?params[1]:"base";
		String table=params!=null&&params.length>2?params[2]:"download_image";
		
		String url=(String)getVarData("preurl");
		String  source=(String)getVarData("source");
		String rowkeyPrefix=(String)getVarData("rowkeyPrefix");
		boolean deleteUrlParam="false".equals((String)getVarData("deleteUrlParam"))?false:true;
		
		String rowkey=SpiderUtil.url2RowKey(url,rowkeyPrefix,deleteUrlParam,deleteUrlParam);
		
		RecordsBean rb=new RecordsBean(family,rowkey,new KVBean("source",source),new KVBean(field,image,true));
		boolean isOK=db.writeData(table,new RecordsBean[]{rb});
		log.info(" save image to table "+table+"."+family+"."+field+" ["+rowkey+"] is  "+(isOK?"success":"fail"));
	}
	
	
}
