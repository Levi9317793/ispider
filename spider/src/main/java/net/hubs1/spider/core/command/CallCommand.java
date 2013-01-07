package net.hubs1.spider.core.command;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.hubs1.spider.core.BaseCommand;
import net.hubs1.spider.core.Command;
import net.hubs1.spider.core.RunCommand;

public class CallCommand extends BaseCommand{
	public CallCommand(int lineNum, String commandName, String[] params,String[] vars) {
		super(lineNum, commandName, params,vars);
	}
	

	public void call(Object... params) {
		
		Map<String,List<Command>> blocks=commandMap();
		Object objs=this.inputData;
		
		String block=((String)params[0]).substring(1);
		String isAll=params.length==3?(String)params[2]:"false";
		String isAlone=params.length>=2?(String)params[1]:"true";
		
		if("true".equalsIgnoreCase(isAlone)){
			call("true".equalsIgnoreCase(isAll),
				 objs,
				 block,
				 blocks);
		}else{
			try {
				RunCommand.execute(blocks.get(block),blocks,this.blockContext,this.taskContext);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private void call(boolean isAll,Object objs, String block,Map<String, List<Command>> blocks){		
		if(objs instanceof Collection && !isAll){
			Collection list=(Collection)objs;
			List<Map<String, Object>> listR=new ArrayList<Map<String, Object>>(list.size());
			int index=0;
			for(Object obj:list){
				if(obj==null) continue;
				Map<String, Object> map=new HashMap<String,Object>();
				map.put("current", obj);
				map.put("current_index",String.valueOf(index++));
				RunCommand.execute(blocks.get(block),blocks,map,this.taskContext);
				map.remove("current");
				map.remove("current_index");
				listR.add(map);
			}
			putResult2Context(listR);
			//ContentUtil.putContent(this.vars, listR, content);
		}else{
			Map<String, Object> map=new HashMap<String,Object>();
			map.put("current", objs);
			RunCommand.execute(blocks.get(block),blocks,map,this.taskContext);
			map.remove("current");
			this.putResult2Context(map);
		}
	}
}
