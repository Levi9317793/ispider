package net.hubs1.spider.web;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author David.Dong
 *	
 * 该类主要是辅助生成FlexGrid for JQuery所能接受的JSON格式的字符串。
 */
public class FlexGridJSONData {
	
	private Map<String,Object> data=new HashMap<String,Object>();
	
	private int page=1;
	private int total=0;
	private List<RowData> rows=new ArrayList<RowData>();
	
	private List<Map<String,Object>> rowList=new ArrayList<Map<String,Object>>();
	
	private String rowid=null;
	private List<String> coldatas=null;
	private String replaceNullChar="\"\"";
	
	private String[] colNames=null;
	
	public FlexGridJSONData(){
		data.put("rows", rowList);
	}
	
	/**
	 * @return
	 */
	public int getPage() {
		return page;
	}
	
	
	
	/**
	 * 设置页码。
	 * @param page
	 */
	public void setPage(int page) {
		this.page = page;
		data.put("page", page);
	}
	
	
	public int getTotal() {
		return this.total;
	}
	
	/**
	 * 设置总记录数
	 * @param total
	 */
	public void setTotal(int total) {
		this.total = total;
		data.put("total", total);
	}
	
	
	public List<RowData> getRows() {
		return rows;
	}



	public void setRows(List<RowData> rows) {
		this.rows = rows;
	}
	
	
	public void addRow(String rowid,List<String> coldatas){
		RowData rd=new RowData(rowid,coldatas);
		this.rows.add(rd);
	}
	
	/**
	 * 设置每一行的id。
	 * 配合addColdata(),commitData()方法是用。 
	 * 例：setRowId("row1");
	 *     addColdata("1");
	 *     addColdata("2");
	 *     
	 *     setRowId("row2");
	 *     addColdata("a");
	 *     addColdata("b");
	 *     
	 *     commitData();
	 *   表示 1，2两个数据都为行row1中第一列，第二列的数据。
	 *  a,b 两个数据都为row2中第一列，第二列的数据。
	 *  commitData()的调用表示，row2行的数据已经组织完成。
	 *  
	 *  在设置row2行的数据时，会自动提交row1行的数据。
	 *  
	 * @param rowid
	 */
	public void setRowId(String rowid){
		commitData();
		this.rowid=rowid;
		this.coldatas=null;
	}
	
	public void setRowId(long rowid){
		commitData();
		this.rowid=String.valueOf(rowid);
		this.coldatas=null;
	}
	
	/**
	 * 该方法配合setRowId和commitData()使用。该方法必须在调用setRowId()后调用，否则会抛出NullPointerException
	 * 请参考setRowId的说明
	 * @param coldata 每一列数据
	 */
	public void addColdata(String coldata){
		addColdata(coldata,true);
	}
	
	
	/**
	 * 设置每一列的名称，及顺序。
	 * @param rowname
	 */
	public void setColName(String... colname ){
		this.colNames=colname;
	}
	
	public void addColdata(String coldata,boolean needOpt){
		if(rowid==null) throw new NullPointerException("please set rowid");
		if(coldatas==null) coldatas=new ArrayList<String>();
		
//		if() coldata=this.replaceNullChar;
		if(coldata!=null&&needOpt) coldata="\""+coldata.replaceAll("\\\"", "\\\\\"")+"\"";
		coldatas.add(coldata);
	}
	
	public void addColdata(Integer coldata){
		this.addColdata(String.valueOf(coldata),false);
	}
	
	public void commitData(){
		if(this.rowid!=null && this.coldatas!=null){
			addRow(this.rowid, this.coldatas);
			this.rowid=null;
			this.coldatas=null;
		}
	}
	
	
	/* 
	 * 这里生成的是符合flexgrid for jquery 的json格式字符串
	 * 其格式如下：
	 * {
		page:1,
		total:0,
		rows:[
		  {id:'row2',cell:['col','col','col','col','col','col']},
		  {id:'row3',cell:['col','col','col','col','col','col']},
		  {id:'row1',cell:['col','col','col','col','col','col']}
		   ]
		}
	 */
	public String toString(){
		StringBuffer sb=new StringBuffer();
		sb.append("{\r\n");
		sb.append("\"page\":").append(page).append(",\r\n");
		
		if(this.rows==null || this.rows.isEmpty()){
			sb.append("\"total\":").append(0).append(",\r\n");
		}else{
			sb.append("\"total\":").append(total).append(",\r\n");
		}
		
		sb.append("\"rows\":[\r\n");
		int keynum=1;
		List<RowData> rowdatalist=this.rows;
		for(RowData rowdata:rowdatalist){
			sb.append(" {\"id\":\"").append(rowdata.getRowid()).append("\",").append("\"cell\":[");
			int i=1;
			List<String> coldatalist=rowdata.getColdata();
			for(String data:coldatalist){
				sb.append(data==null?this.replaceNullChar:data);
				if(i<coldatalist.size()){
					sb.append(",");
				}
				i++;
			}
			
			if(keynum<rowdatalist.size()){
				sb.append("]},\r\n");
			}else{
				sb.append("]}\r\n");
			}
			
			keynum++;
		}
		
		sb.append("  ]\r\n");
		if(this.colNames!=null&&this.colNames.length>0){
			sb.append(",");
			sb.append("\"colNames\":[");
			for(String cn:colNames){
				sb.append("\"").append(cn).append("\",");
			}
			sb.deleteCharAt(sb.length()-1).append("]");
		}
		sb.append("}");
		
//		
		return sb.toString();
	}
	
	
	/* 
	 * 这里生成的是符合flexgrid for jquery 的json改良版的字符串
	 * 其格式如下：
	 * {
		page:1,
		total:0,
		rows:[
		  {rowid:'row2',name:"",}每行对应于一个数据对象。 属性名称，应该和flexigrid列中定义的name字段名字相同，这样就会字段匹配
		  {rowid:'row3',}
		  {rowid:'row4',}
		   ]
		}
		
		如果使用该方法，在初始化flexigrid中加入以下属性及方法。
		preProcess:function(data){
				var datas={};
				datas.page=data.page;
				datas.total=data.total;
				var rows=new Array();
				for(var j=0;j<data.rows.length;j++){
					var d=data.rows[j];
					var row={};
					row.id=d.rowid;
					var cells=new Array();
					for(var i=0;i<this.colModel.length;i++){
						cells[i]=d[this.colModel[i].name];
					}
					row.cell=cells;
					rows[j]=row;
				}
				datas.rows=rows;
				return datas;
			}
		
	 */
	public String toExtString(){
		return map2JSONObject(this.data);
	}
	
	public class RowData{
		String rowid=null;
		List<String> coldata=null;
		
		public RowData(){
			
		}
		
		public RowData(String rowid,List<String> coldata){
			this.rowid=rowid;
			this.coldata=coldata;
		}
		
		public List<String> getColdata() {
			return coldata;
		}
		public String getRowid() {
			return rowid;
		}
		public void setColdata(List<String> coldata) {
			this.coldata = coldata;
		}
		public void setRowid(String rowid) {
			this.rowid = rowid;
		}
	}
	
	
	public static void main(String args[]){
		
		
		FlexGridJSONData fgjd=new FlexGridJSONData();
		fgjd.setRowId("row1");
		fgjd.addColdata("cols");
		fgjd.addColdata("cols");
		fgjd.addColdata("cols");
		
		fgjd.setRowId("row2");
		fgjd.addColdata("cols");
		fgjd.addColdata("cols");
		fgjd.addColdata("cols");
		
		fgjd.setRowId("row3");
		fgjd.addColdata("cols");
		fgjd.addColdata("cols");
		fgjd.addColdata("cols");
		
		fgjd.commitData();
		System.out.println(fgjd.toString());
	}



	public String getReplaceNullChar() {
		return replaceNullChar;
	}

	
	public void addRow(String rowid,Map<String,Object> data){
		data.put("rowid", rowid);
		this.rowList.add(data);
	}

	/**
	 * 当输入的数据为null时，使用什么字符串代替。
	 * 该设置不影响数据本身。只在输出时做一个替换。
	 * 默认为零长度字符串。
	 * @param replaceNullChar
	 */
	public void setReplaceNullChar(String replaceNullChar) {
		this.replaceNullChar = replaceNullChar;
	}

	private String map2JSONObject(Map<String,Object> map){
		StringBuilder sb=new StringBuilder("{");
		for(Map.Entry<String, Object> entry:map.entrySet()){
			sb.append("\"").append(entry.getKey()).append("\":");
			if(entry.getValue()==null){
				sb.append("\"\",");
				continue;
			}
			if(entry.getValue() instanceof Map){
				String obj=map2JSONObject((Map<String,Object>)entry.getValue());
				sb.append(obj).append(",");
				continue;
			}
			if(entry.getValue() instanceof List){
				String obj=list2JSONList((List<Object>)entry.getValue());
				sb.append(obj).append(",");
				continue;
			}
			sb.append("\"").append(entry.getValue().toString()).append("\",");
		}
		if(sb.length()>1) sb.deleteCharAt(sb.length()-1);
		sb.append("}");
		return sb.toString();
	}
	
	private String list2JSONList(List<Object> list){
		StringBuilder sb=new StringBuilder("[");
		for(Object obj:list){
			if(obj==null){
				continue;
			}
			if(obj instanceof Map){
				String map=map2JSONObject((Map<String,Object>)obj);
				sb.append(map).append(",");
				continue;
			}
			if(obj instanceof List){
				String ll=list2JSONList((List<Object>)obj);
				sb.append(ll).append(",");
				continue;
			}
			sb.append("\"").append(obj.toString()).append("\",");
		}
		if(sb.length()>1) sb.deleteCharAt(sb.length()-1);
		sb.append("]");
		return sb.toString();
	}

	
}
