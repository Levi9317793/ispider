package net.hubs1.spider.core;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class TaskLine implements Runnable{
	Queue<Task> tasks=new ArrayBlockingQueue<Task>(10,true);
	  
	public void registerTask(String format){
		tasks.add(new Task(format));
	}

	@Override
	public void run() {
		while(!tasks.isEmpty()){
			Task t=tasks.poll();
			t.execute();
		}		
	}
}
