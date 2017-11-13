package com.ucloudlink.css.main;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.ucloudlink.css.canal.CanalFactory;
import com.ucloudlink.css.canal.MonitorInfo;
import com.ucloudlink.css.canal.MutiCanalFactory;
import com.ucloudlink.css.common.BaseUtil;
import com.ucloudlink.css.util.NumberUtil;

public class Main {
	private static Logger logger = LogManager.getLogger();
	private static CanalFactory factory = BaseUtil.single();
	private static MutiCanalFactory mfactory = BaseUtil.muti();
	/**
	 * 访问方式:0.Single方式,1.Muti方式
	 */
	private static int ACCESS_TYPE = 0;
	/**
	 * 计数器
	 */
	private static AtomicInteger atomic = new AtomicInteger(0);
	
	public static void read(long count){
		logger.info("--Read Count:"+count);
		long start = System.currentTimeMillis();
		for(int i=0;(count<1?true:i<count);i++){
			List<MonitorInfo> result = null;
			if(ACCESS_TYPE==0)result = factory.execute();
			if(ACCESS_TYPE==1)result = mfactory.service();
			for (MonitorInfo monitor: result) {
				logger.info("--Canal Read Data:------------"+JSON.toJSONString(monitor));
			}
			atomic.incrementAndGet();
			long end = System.currentTimeMillis();
			double time = (end - start) / 1000.00;
			if(time>=100){
				System.exit(1);
			}
			double ss = time % 60;
			int mm = Double.valueOf(time / 60).intValue() % 60;
			int hh = Double.valueOf(time / 60 / 60).intValue() % 60;
			logger.info("["+atomic.get()+"]ES Read 耗时:"+(hh>0?hh+"小时":"")+(mm>0?mm+"分钟":"")+ss+"秒-------------"+result.size());
		}
	}
	
	public static void thread(final long count,final int thread,final int type){
		ACCESS_TYPE=type;
		int cpu = Runtime.getRuntime().availableProcessors();
		for(int i=0;(thread<1?true:i<thread);i++){
			Thread service = new Thread(new Runnable() {
				@Override
				public void run() {
					read(count);
				}
			});
			service.setName(cpu+"-Elasticsearch-Thread-"+i);
			service.start();
		}
	}
	public static void execute(final long count,final int thread,final int type){
		ACCESS_TYPE=type;
		ExecutorService service = Executors.newSingleThreadExecutor();
		if(thread>1){
			service = Executors.newFixedThreadPool(thread);
		}else{
			if(thread<1){
				service = Executors.newCachedThreadPool();
			}
		}
		for(int i=0;(thread<1?true:i<thread);i++){
			service.submit(new Runnable() {
				@Override
				public void run() {
					read(count);
				}
			});
		}
	}
	public static void main(String[] args) {
		long count = args!=null&&args.length>1&&NumberUtil.isNumber(args[0])?Long.valueOf(args[0]):0;
		int thread = args!=null&&args.length>3&&NumberUtil.isNumber(args[2])?Integer.valueOf(args[2]):1;
		int type = args!=null&&args.length>4&&NumberUtil.isNumber(args[3])?Integer.valueOf(args[3]):0;
		logger.info("--{count:"+count+",thread:"+thread+",type:"+type+"[0.Single方式,1.Muti方式]}--"+JSON.toJSONString(args));
		if(args!=null&&args.length>3){
			execute(count, thread, type);
		}else{
			thread(count, thread, type);
		}
	}
}
