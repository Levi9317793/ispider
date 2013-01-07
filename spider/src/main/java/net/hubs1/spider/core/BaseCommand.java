package net.hubs1.spider.core;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import net.hubs1.spider.core.command.CallCommand;
import net.hubs1.spider.core.command.ConditionCommand;
import net.hubs1.spider.core.command.DateCommand;
import net.hubs1.spider.core.command.EndConditionCommand;
import net.hubs1.spider.core.command.ExtractHTMLCommand;
import net.hubs1.spider.core.command.ExtractStringCommand;
import net.hubs1.spider.core.command.JsonCommand;
import net.hubs1.spider.core.command.RequestCommand;
import net.hubs1.spider.core.command.DBCommand;
import net.hubs1.spider.core.command.SetCommand;
import net.hubs1.spider.core.command.ThreadCommand;
import net.hubs1.spider.core.command.ToolCommand;
import net.hubs1.spider.util.ContentUtil;

public abstract class  BaseCommand implements Command,Call,ResourceClose{
	
	public final static String CURRENT_VALUE="current";
	public final static String PRE_URL="preurl";
	
	private static Logger log=Logger.getLogger(BaseCommand.class);
	protected String[] paramStrs=null;//模版输入参数，某参数可能仅仅表示一个变量名称，在实际使用时，需转化。
	protected String inputVar=null;
	private Method method=null;
	
	protected int lineNum=0;
	protected String commandName=null;
	
	protected String[] vars=null;
	protected Object inputData=null;
	
	protected String rsTag="command";
	
	private static Map<String,Class<? extends BaseCommand>> map=new HashMap<String,Class<? extends BaseCommand>>();
	protected  Map<String,Object> taskContext=null;//new HashMap<String,Object>();
	protected  Map<String,Object> blockContext=null;
	protected Map<String,List<Command>> commandMap=null;
	
	static{
		map.put("condition", ConditionCommand.class);
		map.put("loop", ConditionCommand.class);
		map.put("endcondition", EndConditionCommand.class);
		map.put("html", ExtractHTMLCommand.class);
		map.put("htmle", ExtractHTMLCommand.class);
		map.put("htmlgroup", ExtractHTMLCommand.class);
		map.put("htmlfilter", ExtractHTMLCommand.class);
		map.put("htmlextract", ExtractHTMLCommand.class);
		
		map.put("date", DateCommand.class);
		
		map.put("jsonstring", JsonCommand.class);
		map.put("jsonobj", JsonCommand.class);
		map.put("json", JsonCommand.class);
		
		map.put("substring", ExtractStringCommand.class);
		map.put("replace", ExtractStringCommand.class);
		map.put("removestr", ExtractStringCommand.class);
		map.put("extract", ExtractStringCommand.class);
		map.put("split", ExtractStringCommand.class);
		map.put("join", ExtractStringCommand.class);
		map.put("filter", ExtractStringCommand.class);
		map.put("scanurl", DBCommand.class);
		map.put("post",RequestCommand.class );
		map.put("get",RequestCommand.class);
		map.put("saveurl",DBCommand.class );
		map.put("savepage",DBCommand.class );
		map.put("savemap",DBCommand.class );
		map.put("saveimage",DBCommand.class );
		map.put("setgobal", SetCommand.class);
		map.put("set", SetCommand.class);
		map.put("cal", SetCommand.class);
		
		map.put("removeset", SetCommand.class);
		map.put("setrequestparam", SetCommand.class);
		
		map.put("removerequestparam", SetCommand.class);
		map.put("multithread", ThreadCommand.class);
		map.put("call", CallCommand.class);
		map.put("pinyin", ToolCommand.class);
		map.put("list", ToolCommand.class);
		map.put("createurl", ToolCommand.class);
		map.put("url2rowkey", ToolCommand.class);
		map.put("rowkey2url", ToolCommand.class);
		map.put("md5", ToolCommand.class);
		map.put("loop", ToolCommand.class);
		map.put("test", ToolCommand.class);
		map.put("runjs", ToolCommand.class);
		map.put("calculate", ToolCommand.class);
		map.put("table2string", ToolCommand.class);
	}
	
	public BaseCommand(int lineNum,String commandName,String[] params,String[] vars){
		this.lineNum=lineNum;
		this.commandName=commandName.toLowerCase();
		this.inputVar=getInputVar(params);
		this.paramStrs=getMethodRealInputParam(params);
		this.vars=vars;
		if(this.vars==null||this.vars.length==0) this.vars=new String[]{CURRENT_VALUE};
	}
	
	
	public void setContext(Map<String,Object> block,Map<String,Object> gobal){
		this.blockContext=block;
		this.taskContext=gobal;
	}
	
	/**
	 * 获取该命令的数据来源变量名称
	 * @param params
	 * @return
	 */
	private  String getInputVar(String[] params){
		if(params==null||params.length==0){
			return CURRENT_VALUE;
		}
		if(params[0].startsWith("$")){
			return params[0].substring(1);
		}
		return CURRENT_VALUE;
	}
	
	
	public BaseCommand(){
		
	}
	
	
	public static boolean exists(String commandName){
		return null!=map.get(commandName.toLowerCase());
	}
	
	public static BaseCommand checkSet(int lineNum,String commandName,String paramstr,String varstr)throws Exception{
		Class<? extends BaseCommand> c=map.get(commandName.toLowerCase());
		if(c==null){
			log.error(" don't define command:"+commandName+" at line number:"+lineNum);
			return null;
		}else{
			String[] params=getParams(paramstr,lineNum);
			String[] vars=getVars(varstr, lineNum);
			Constructor constructor=c.getConstructor(int.class,String.class,String[].class,String[].class);
			return (BaseCommand)constructor.newInstance(lineNum,commandName,params,vars);
			
		}
	}
	
	
	private static String[] getParams(String paramStr,int lineNum){
		if(paramStr==null||paramStr.length()==0) return null;
		String[] params=paramStr.trim().split("\",\"");
		if(!params[0].startsWith("\"")||!params[params.length-1].endsWith("\"")){
			log.error("  params loss \" "+lineNum);
			throw new RuntimeException("  params loss \" "+lineNum );
		}
		params[0]=params[0].substring(1);
		params[params.length-1]=params[params.length-1].substring(0,params[params.length-1].length()-1);
		return params;
	}
	
	private static String[] getVars(String varstr,int lineNum){
		if(varstr==null||varstr.length()==0) return null;
		String[] vars=varstr.trim().split(",");
		for(int i=0;i<vars.length;i++){
			if(!vars[i].startsWith("$")){
				log.error(" var must startWith $ ,at line number :"+lineNum);
				throw new RuntimeException(" var must startWith $ ,at line number :"+lineNum );
			}
			vars[i]=vars[i].substring(1);
		}
		return vars;
	}
	
	@Override
	public  void execute(Object... params){
		log.debug("start execute  the "+this.commandName+" (at linenum "+this.lineNum+"), params is "+params2String());
		if(method==null){
			try {
				Method[] methods=this.getClass().getMethods();
				for(Method m:methods){
					if(m.getName().equalsIgnoreCase(this.commandName)){
						this.method=m;
						break;
					}
				}
			} catch (Exception e) {
				log.error(e);
			}
		}
		
		this.inputData=ContentUtil.getVarData(this.inputVar, blockContext,taskContext);
		
		try{
			Object[] objs=params==null||params.length==0?params2Object():params;
			String[] strp=null;
			if(objs!=null&&objs.length>0){
				Class<?> type=method.getParameterTypes()[0];
				if(type==String[].class){
					strp=new String[objs.length];
					for(int index=0;index<objs.length;index++){
						strp[index]=(String)objs[index];
					}
				}
			}
			
			method.invoke(this,(Object)(strp==null?objs:strp));
		}catch(Exception e){
			log.error(" command invoke is fail,the command is ["+this.commandName+"] the line num is:"+this.lineNum,e);
		}
	}
	
	/**
	 * 将字符参数转为实际的参数。
	 * @return
	 */
	private <T> Object[] params2Object(){
		if(this.paramStrs==null||this.paramStrs.length==0) return null;
		Object[] pos=new Object[this.paramStrs.length];
		for(int i=0;i<pos.length;i++){
			pos[i]=ContentUtil.getParamVale(this.paramStrs[i], blockContext,taskContext);
		}
		return pos;
	}
	
	
	private String params2String(){
		if(this.paramStrs==null||this.paramStrs.length==0) return "null";
		
		StringBuilder sb=new StringBuilder("[");
		for(String p:paramStrs){
			sb.append(p).append(",");
		}
		if(sb.length()>1) sb.deleteCharAt(sb.length()-1);
		sb.append("]");
		return sb.toString();
	}
	
	private String[] getMethodRealInputParam(String[] params){
		if(params==null||params.length==0) return new String[0];
		if(params[0].startsWith("$")){
			String[] r=new String[params.length-1];
			System.arraycopy(params, 1, r, 0, r.length);
			return r;
		}else{
			return params;
		}
	}
	
	/**
	 * 当命令的数据源的数据为null时，是否需要继续调用Command
	 * @return
	 */
	protected boolean nullContinue(){
		return false;
	}
	
	
	/**
	 * 当指定当前命令是否需要设置当前变量的值。
	 * @return
	 */
	protected boolean needSetVar(){
		return true;
	}
	
	protected void putResult2Context(Object value) {
		ContentUtil.putContent(this.vars, value, blockContext);
	}
	
	protected void setContextVar(String key,Object value) {
		this.blockContext.put(key, value);
	}
	
	protected void setGobalVar(Object value,String... keys) {
		ContentUtil.putContent(keys, value, taskContext);
	}
	
	protected void setGobalVar(Object value) {
		ContentUtil.putContent(this.vars, value, taskContext);
	}
	
	public String commandName(){
		return this.commandName;
	}
	
	public String lineNum(){
		return this.lineNum();
	}
	
	public String[] paramsStrs(){
		return this.paramStrs;
	}
	
	@Override
	public Map<String,List<Command>> commandMap(){ 
		return this.commandMap;
	}
	
	public void setCommandMap(Map<String,List<Command>> commands){
		this.commandMap=commands;
	}
	 /**
	 * 在给定的上下文中获取指定的变量的值
	 * @param var
	 * @param content
	 * @return
	 */
	protected  Object getVarData(String var){
		return ContentUtil.getVarData(var, blockContext,this.taskContext);
	}

	public void destoryResource(){
		
	}
}
