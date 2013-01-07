package net.hubs1.spider.core;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import net.hubs1.spider.core.config.SpiderContext;
import net.hubs1.spider.store.KVBean;
public class Task implements SpiderTask{
	private static Logger log=Logger.getLogger("Task");
	private Map<String,Object>  tcontent=null;
	private Map<String,Object>  gobalContent=null;
	private Map<String,List<Command>> commandMap=null;
	
	private ExecutorService threadPools = null;// 
	private Object[] queue=null;
	private int threadNum=0;
	
	
	public Task(String formatFilePath){
		commandMap=CommandParse.parse(formatFilePath);
		if(commandMap==null||commandMap.isEmpty()) throw new RuntimeException("format parse is error!");
		this.tcontent=new HashMap<String, Object>();
		this.gobalContent=new HashMap<String, Object>();
	}
	
	public Task(Map<String,List<Command>> commands,KVBean kv){
		commandMap=commands;
		if(commandMap==null||commandMap.isEmpty()) throw new RuntimeException(" command is error!");
		this.tcontent=new HashMap<String, Object>();
		this.gobalContent=new HashMap<String, Object>();
		if(kv!=null) this.queue=new KVBean[]{kv};
	}
	
	public void execute(){
		this.init();
		Object o=tcontent.get(BaseCommand.CURRENT_VALUE);
		if(o!=null&&o instanceof List){
			queue=mutiRequest(this.tcontent);
			log.info(" have url number is :"+queue.length);
		}
		tcontent.put(BaseCommand.CURRENT_VALUE, null);
		if(threadPools!=null&&threadNum>1){
			for(Object obj:queue){
				Map<String,Object> c=new HashMap<String, Object>();
				Map<String,Object> g=new HashMap<String, Object>();
				c.putAll(tcontent);
				ThreadTask tt=null;
				if(obj instanceof KVBean){
					KVBean kb=(KVBean)obj;
					tt=new ThreadTask(c, g,kb.getValue(), kb.getKey());
				}else{
					if(obj instanceof String)tt=new ThreadTask(c, g,(String)obj,null);
				}
				if(tt!=null)
					threadPools.execute(tt);
				else
					log.error("dong't know type:"+obj.toString());
			}
			//threadPools.shutdown();//关闭线程池
		}else{
			if(queue==null||queue.length==0) 
				run(null,null);
			else
				for(Object obj:queue){
					if(obj instanceof KVBean){
						KVBean kb=(KVBean)obj;
						run(kb.getValue(),kb.getKey());
					}else{
						run((String)obj,null);
					}
				}
		}
		this.destory();
	}
	
	public void test(String url,String saveFilePath){
		if(url!=null){
			KVBean kv=new KVBean("test",url);
			this.queue=new KVBean[]{kv};
		}
		
		this.execute();
		
	}
	
	
	
	private Object[] mutiRequest(Map<String,Object> content){
		Object urlStr=content.get(BaseCommand.CURRENT_VALUE);		
		Object[] urls=new String[0];
		
		if(urlStr instanceof String){
			String ustr=(String)urlStr;
			urls=ustr!=null?ustr.split(","):new String[0];
		}else{
			Collection list=(Collection)urlStr;
			if(list==null||list.isEmpty()) return new String[0];
			
			urls=list.toArray(new Object[list.size()]);
		}
		return urls;
	}
	
	@Override
	public void init() {
		 List<Command> init=commandMap.get("init");
		 if(init!=null){
			for(Command c:init){
				BaseCommand bc=(BaseCommand) c;
				if("multithread".equals(bc.commandName())){
					threadNum=Integer.parseInt(bc.paramsStrs()[0]);
					threadPools=Executors.newFixedThreadPool(threadNum);
					break;
				}
			}
			try {
				RunCommand.execute(init,null, this.tcontent,this.gobalContent);
			} catch (Exception e) {
				e.printStackTrace();
			}
		 }
	}


	@Override
	public void run(String url,String rowkey) {
		List<Command> run=commandMap.get("run");
		if(run==null||run.isEmpty())
			log.error("don't define run method, that is required!");
		else
			try {
				this.tcontent.put(BaseCommand.CURRENT_VALUE, url);
				RunCommand.execute(run,commandMap, tcontent,this.gobalContent);
			} catch (Exception e) {
				e.printStackTrace();
			}
	}
	
	
	@Override
	public void destory() {
		// TODO Auto-generated method stub
	}
	
	public static void main(String[] args) {
		SpiderContext.initSpiderContentByDefaultFile();
		Task t=new Task(Task.class.getClassLoader().getResource("test.txt").getFile());
		t.test(null,null);
	}
	
	class ThreadTask implements Runnable{
		private Map<String,Object>  content=null;
		private Map<String,Object>  gcontent=null;
		private String url=null;
		private String rowkey=null;
		ThreadTask( Map<String,Object>  content, Map<String,Object>  gcontent,String url,String rowkey){
			this.content=content;
			this.gcontent=gcontent;
			this.content.put(BaseCommand.CURRENT_VALUE, url);
			this.url=url;
			this.rowkey=rowkey;
		}
		@Override
		public void run() {
			List<Command> run=commandMap.get("run");
			if(run==null||run.isEmpty())
				log.error("don't define run method, that is required!");
			else
				try {
					RunCommand.execute(run, commandMap,this.content,this.gcontent);
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
	}
}
