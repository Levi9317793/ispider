$(function() {
		$( "#spider_tabs" ).tabs();
		$( "#newtask_accordion" ).accordion();
		$('.addspeech').speechbubble();
		//$( "#help_accordion" ).accordion();
		$("#tasktable").flexigrid({
			url:"/spider/manager/task?method=taskList",
			dataType: 'json',
			colModel : [
                {display: '任务ID', name : 'rowid', width : 90, sortable : true, align: 'center'},
                {display: '后置任务', name : 'nexttask', width : 90, sortable : true, align: 'center'},
				{display: '任务名称', name : 'name', width : 180, sortable : true, align: 'center'},
				//{display: '描述描述', name : 'desc', width : 280, sortable : true, align: 'left'},
				{display: '任务类型', name : 'type', width : 50, sortable : true, align: 'left'},
				{display: '模版文件', name : 'file', width : 300, sortable : true, align: 'left'},
				{display: '增量', name : 'increment', width : 30, sortable : true, align: 'right'},
				{display: '间隔', name : 'fixedtime', width : 30, sortable : true, align: 'right'},
				{display: '已执行', name : 'runnum', width : 50, sortable : true, align: 'right'},
				{display: '当前状态', name : 'cstatus', width : 80, sortable : true, align: 'right'},
				//{display: '首次执行时间', name : 'firstruntime', width : 100, sortable : true, align: 'right'},
				//{display: '上一次执行时间', name : 'preruntime', width : 100, sortable : true, align: 'right'},
				//{display: '上一次执行结果', name : 'prerunresult', width : 100, sortable : true, align: 'right'},
				{display: '下一次执行时间', name : 'nextruntime', width : 100, sortable : true, align: 'right'},
				//{display: '数据来源', name : 'source', width : 100, sortable : true, align: 'right'},
				{display: '数据范围[起始]', name : 'startrange', width : 150, sortable : true, align: 'right'},
				{display: '数据范围[结束]', name : 'endrange', width : 150, sortable : true, align: 'right'},
				//{display: '创建IP', name : 'creatorIP', width : 100, sortable : true, align: 'right'}
				],
			buttons : [
				{name: '创建任务', bclass: 'flexigrid_add',onpress:showAddTaskDialog},
				{separator: true},
				{name: '删除任务', bclass: 'flexigrid_delete',onpress:deleteTask},
				{separator: true},
				{name: '执行任务', bclass: 'flexigrid_ok',onpress:runTask},
				{separator: true}
				],
				
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
						cells[i]=d[this.colModel[i].name]?d[this.colModel[i].name]:"";
					}
					row.cell=cells;
					rows[j]=row;
				}
				datas.rows=rows;
				return datas;
			},
			usepager: true,
			title: '任务列表',
			useRp: false,
			showTableToggleBtn: false,
			width: 'auto',
			height: 450
		});   
		initMenu();
//		 $('#switchcss').themeswitcher();
//		 $("#find_task" ).jsRightMenu({
//			    menuList: [{ menuName: "修改", clickEvent: "divClick('1')"},
//			               { menuName: "删除", clickEvent: "divClick('2')"},
//			               { menuName: "复制", clickEvent: "divClick('3')"},
//			               { menuName: "移动", clickEvent: "divClick('4')"}
//			              ]
//			    });
	});
	
	function setPath(type){
		$("#formatsavepath").val("/spider/formats/"+type+"/");
	}
	
	function submitFormat(){
		$.post("/spider/manager/new",
			$("#newFormat").serialize(),
			function(data) {
			 $("#submit_result").html(data);
			 $( "#dialog" ).dialog({title:"提交结果"});
			}
		);
	}
	
	
	function createTask(isRun){
		if(isRun){
			document.newtaskform.isrun.value="true";
		}else{
			document.newtaskform.isrun.value="false";
		}
		$.post("/spider/manager/newtask",
			$("#newtaskform").serialize(),
			function(data) {
			 $("#submit_result").html(data);
			 $("#tasktable").flexReload({newp:1});
			 $( "#dialog" ).dialog({title:"提交结果"});
			}
		);
	}
	
	function runTask(){
		var id=$(".flexigrid").find(".trSelected").attr("id");
		$.post("/spider/manager/runtask",
				{method:"runTask",rowkey:id},
				function(data) {
				 $("#submit_result").html(data);
				 $( "#dialog" ).dialog({title:"提交结果"});
				}
			);
	}
	
	function deleteTask(){
		var id=$("#tasktable").find(".trSelected").attr("id");
		$.post("/spider/manager/deletetask",
				{method:"deleteTask",rowkey:id},
				function(data) {
				 $("#submit_result").html(data);
				 //$( "#dialog" ).dialog({title:"提交结果"});
				 $("#tasktable").flexReload({newp:1});	
				}
			);
	}
	
	function listFile(file){
		if(file.data){
			file=file.data[0];
		}
		$("#sub_list").html("<li>&nbsp;</li>");
		
		$.getJSON("/spider/manager/list",
				{method:"listFile",basepath:file},
				function(data) {
					var isShow=false;
					var obj=data;
					if(1==obj.type){
						$("#submit_result" ).html(obj.content);
						$( "#dialog" ).dialog({
							title:file,
							minWidth:1024,
							show: "blind",
							hide: "explode"
							});
						isShow=true;
					}
					if(0==obj.type){
						if(obj.list&&obj.list.length>0){
							var sd=$( "#subdir_dialog" ).clone();
							$(sd).find("#sub_list").html("  ");
							
							for(var i=0;i<obj.list.length;i++){
								var sub=obj.list[i];
								
								if(sub.isDir){
									var e=$("#dir_format").clone();
									$(e).show();
									//$(e).click([file+"/"+sub.name],listFile);
									$(e).mouseup([file+"/"+sub.name,"dir"],fileOpt);
									$(e).find("#dir_name").html(sub.name);
									var li=$("<li></li>").append(e);
									$(sd).find("#sub_list").append(li);
								}else{
									var e=$("#file_format").clone();
									$(e).show();
									$(e).mouseup([file+"/"+sub.name,"file"],fileOpt);
									$(e).find("#file_name").html(sub.name);
									//$(e).find("#file_img").bind(sub.name,listFile);
									
									var li=$("<li></li>").append(e);
									$(sd).find("#sub_list").append(li);
								}
							}
							$( sd ).dialog({
								title:file,
								minWidth:1024,
								show: "blind",
								hide: "explode"
								});
							isShow=true;
						}
					}
					if(!isShow)$("#subdir_dialog").dialog({title:file});
				 }
			 );
	}
	
	function startTest(){		
		$("#out_result").html("");
		$.get("/spider/manager/test",
				$("#testFormat").serialize(),
				function(data) {
				  $("#out_result").html(data);
				}
			);
	}
	
	function createPath(){
		var formE=document.getElementById("newFormat");
		var base=formE.basepath.value;
		var extp=formE.extp.value;
		var file=formE.fileName.value;
		var path=base+extp+"/"+file;
		document.getElementById("testFormat").formatFilePath.value=base+extp+"/"+file;
	}
	
	function showAddTaskDialog(){
		fillTaskSelect();
		$("#create_task").dialog({
			minWidth:800,
			title:"创建新任务",
			modal: true,
			buttons: {
				"提交": function() {
					createTask(false),
					$( this ).dialog( "close" );
				},
				"提交并立即执行": function() {
					createTask(true),
					$( this ).dialog( "close" );
				}
			}
		});
	}
	
	function selectData(type){
		if("extra"==type){
			$("#format_file_p,#datarange_p").show();
			$("#format_file_p,#datarange_p").find("input").each(function(){
				$(this).attr("disabled",null);
			})
			
			$("#sourcepath_p").hide();
			$("#sourcepath_p").find("input").each(function(){
				$(this).attr("disabled","disabled");
			})
		}
		
		if("multi"==type){
			$("#sourcepath_p,#datarange_p").show();
			$("#sourcepath_p,#datarange_p").find("input").each(function(){
				$(this).attr("disabled",null);
			})
			
			$("#format_file_p").hide();
			$("#format_file_p").find("input").each(function(){
				$(this).attr("disabled","disabled");
			})
		}
		
		if("single"==type){
			$("#format_file_p").show();
			$("#format_file_p").find("input").each(function(){
				$(this).attr("disabled",null);
			})
			
			$("#datarange_p,#sourcepath_p").hide();
			$("#datarange_p,#sourcepath_p").find("input").each(function(){
				$(this).attr("disabled","disabled");
			})
		}
	}
	
	function fillTaskSelect(){
		$("#pre_task_select,#next_task_select").find("option[rm]").remove();
		$.getJSON("/spider/manager/tasklist",
				{method:"taskList",taskName:"name"},
				function(data) {
					for(var j=0;j<data.rows.length;j++){
						var d=data.rows[j];
						var id=d.rowid;
						var name=d["name"];
						var optione=$("<option rm='need' value='"+id+"'>"+name+"</option>");
						$("#pre_task_select").append(optione);
						$("#next_task_select").append(optione.clone());
					}
				}
			);
	}
	function fileOpt(obj){
		if(obj.which){
			if(obj.which==3) showMenu(obj);
			if(obj.which==1) listFile(obj);
			return false;
		}else{
			listFile(obj);
		}
	}
	
	function initMenu(){
	    $(".rightmenu_opt").each(function(){
	    	 $(this).bind("mouseover", function() {
                 this.style.backgroundColor = "#316ac5";
             });
             $(this).bind("mouseout", function() {
                 this.style.backgroundColor = '#EAEAEA';
             });
	    })
	    
	    $("#right_menu").bind("mouseleave",function(e){
	        $("#right_menu").hide("slow");
	    	return false;
        });
	    
	    $("#find_task>").find("div[data]").each(function(){
	    	var data=$(this).attr("data");
	    	$(this).mouseup([data,"dir"],fileOpt);
	    });
	    
	    $(this).bind('contextmenu', function(e) {      
            return false;
        });
	    
	}
	
	function showMenu(){
		var event = arguments[0] || window.event;
		if(event.which!=3) return false;
		
		var isDir=event.data[1]=="dir";
		var path=event.data[0];
		
		
		$("#right_menu").find("li").each(function(){
			$(this).attr("data",path);
		});
		
		if(isDir){
			$("#right_menu").find(".rightmenu_file").each(function(){
				$(this).hide();
			});
		}else{
			$("#right_menu").find(".rightmenu_file").each(function(){
				$(this).show();
			});
		}
		
		var objMenu=$("#right_menu");
        var clientX = event.clientX;
        var clientY = event.clientY;
        
        var redge = document.body.clientWidth - clientX;
        var bedge = document.body.clientHeight - clientY;
        var menu = objMenu.get(0);
        var menuLeft = 0;
        var menuTop = 0;
        if (redge < menu.offsetWidth)
            menuLeft = document.body.scrollLeft + clientX - menu.offsetWidth;
        else
            menuLeft = document.body.scrollLeft + clientX;
        if (bedge < menu.offsetHeight)
            menuTop = document.body.scrollTop + clientY - menu.offsetHeight;
        else
            menuTop = document.body.scrollTop + clientY;
        
        objMenu.css({ top: menuTop + "px", left: menuLeft + "px" });
        
        
        objMenu.show("slow");
        
        return false;
	}
	


	/* e即为事件，target即为绑定事件的节点 */

	function fixedMouse(e,target){  

	        var related,

	            type=e.type.toLowerCase();//这里获取事件名字

	        if(type=='mouseover'){

	            related=e.relatedTarget||e.fromElement

	        }else if(type='mouseout'){

	            related=e.relatedTarget||e.toElement

	        }else return true;

	        return related && related.prefix!='xul' && !contains(target,related) && related!==target;

	    }



	/* p=parentNode, c=childNode */

	function contains(p,c){  
	    return p.contains ? 
	           p != c && p.contains(c) :
	           !!(p.compareDocumentPosition(c) & 16);  
	}

	
	function menuOpt(type,element){
		var path=$(element).attr("data");
		if(!path) return false;
		$("#menu_opt_dialog_input" ).attr("rows",1);
		switch(type){
			case 1://查看
				$.get("/spider/manager/fsOpt",
					{method:"fsOpt",path:path,type:"view"},
					function(data) {
						$("#submit_result" ).html(data);
						$( "#dialog" ).dialog({
							title:"查看文件:"+path,
							minWidth:1024,
							show: "blind",
							hide: "explode"
							});
					});
					break;
			case 2://复制
				$( "#menu_opt_dialog" ).dialog({
					title:"请输入复制的目标目录",
					minWidth:624,
					show: "blind",
					hide: "explode",
					buttons: {
						"确定": function() {
							var targetp=$("#menu_opt_dialog_input").val();
							if(!targetp||targetp.length<1) return false; 
							$.post("/spider/manager/copy",
									{method:"fsOpt",path:path,type:"copy",target:targetp},
									function(data) {
										$("#submit_result" ).html(data);
										$( "#dialog" ).dialog({title:"修改"});
									});
							$(this).dialog("close");
						}
					}
					});
				break;
			case 3://修改
				$.get("/spider/manager/fsOpt",
						{method:"fsOpt",path:path,type:"edit"},
						function(data) {
							$("#menu_opt_dialog_input" ).attr("rows",20);
							$("#menu_opt_dialog_input" ).val(data);
							
							$( "#menu_opt_dialog" ).dialog({
								title:"修改文件:"+path,
								minWidth:824,
								show: "blind",
								hide: "explode",
								buttons: {
									"确定": function() {
										var targetp=$("#menu_opt_dialog_input").val();
										if(!targetp||targetp.length<1) return false; 
										$.post("/spider/manager/fsOpt",
												{method:"fsOpt",path:path,type:"save",format:targetp},
												function(data) {
													$("#submit_result" ).html(data);
													$( "#dialog" ).dialog({title:"修改"});
												});
										$("#menu_opt_dialog_input" ).attr("rows",1);
										$("#menu_opt_dialog" ).dialog("close");
									}
								}
								});
						});
						break;
			case 4://移动
				$( "#menu_opt_dialog" ).dialog({
						title:"移动",
						minWidth:824,
						show: "blind",
						hide: "explode",
						buttons: {
							"确定":function(){
								var targetp=$("#menu_opt_dialog_input").val();
								if(!targetp||targetp.length<1) return false; 
								$.post("/spider/manager/move",
										{method:"fsOpt",path:path,type:"move",target:targetp},
										function(data) {
										 $("#submit_result").html(data);
										 $( "#dialog" ).dialog({title:"移动(如成功，请刷新页面)"});
										});
								$(this).dialog("close");
							}
						}
					});
				break;
			case 5://删除
				 $("#submit_result_delete").html("确定要删除？");
				 $( "#dialog_delete" ).dialog({
						    title:"删除",
							minWidth:524,
							show: "blind",
							hide: "explode",
							buttons: {
								"确定删除":function(){
									$.post("/spider/manager/delete",
											{method:"fsOpt",path:path,type:"delete"},
											function(data) {
												 $("#submit_result").html(data);
												 $( "#dialog" ).dialog({title:"删除(如成功，请刷新页面)"});
											});
									$(this).dialog("close");
								}
							}
				 });
				 	
				break;
			case 6://新建目录
				$( "#menu_opt_dialog" ).dialog({
					title:"创建目录",
					minWidth:1024,
					show: "blind",
					hide: "explode",
					buttons: {
						"确定":function(){
							var targetp=$("#menu_opt_dialog_input").val();
							if(!targetp||targetp.length<1) return false; 
							$.post("/spider/manager/mkdir",
									{method:"fsOpt",path:path,type:"mkdir",target:targetp},
									function(data) {
									 $("#submit_result").html(data);
									 $( "#dialog" ).dialog({title:"创建目录(如成功，请刷新页面)"});
									}
							);
							$(this).dialog("close");
						}
					}
				});
				break;
			default:
				break;
				
		}
	}

	