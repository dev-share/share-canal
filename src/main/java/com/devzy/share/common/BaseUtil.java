package com.devzy.share.common;

import com.devzy.share.canal.CanalFactory;
import com.devzy.share.canal.MutiCanalFactory;
import com.devzy.share.util.StringUtil;
///**
// * @decription 配置文件获取数据源工厂
// * @author yi.zhang
// * @time 2017年7月31日 下午12:03:10
// * @since 1.0
// * @jdk	1.8
// */
public class BaseUtil {
	
	public static CanalFactory single(){
		try {
			String destination = CanalConfig.getProperty("canal.destination");
			String servers = CanalConfig.getProperty("canal.servers");
			String username = CanalConfig.getProperty("canal.username");
			String password = CanalConfig.getProperty("canal.password");
			String batch_size = CanalConfig.getProperty("canal.batch_size");
			String filter_regex = CanalConfig.getProperty("canal.filter_regex");
			String isZookeeper = CanalConfig.getProperty("canal.zookeeper.enabled");
			CanalFactory factory = new CanalFactory(destination, servers, username, password);
			if(!StringUtil.isEmpty(batch_size)||!StringUtil.isEmpty(filter_regex)||!StringUtil.isEmpty(isZookeeper)){
				if(!StringUtil.isEmpty(batch_size)){
					factory.setBatch_size(Integer.valueOf(batch_size));
				}
				if(!StringUtil.isEmpty(filter_regex)){
					factory.setFilter_regex(filter_regex);
				}
				if(!StringUtil.isEmpty(isZookeeper)){
					factory.setZookeeper(Boolean.valueOf(isZookeeper));
				}
			}
			return factory;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static MutiCanalFactory muti(){
		try {
			String destinations = CanalConfig.getProperty("canal.destinations");
			String servers = CanalConfig.getProperty("canal.servers");
			String username = CanalConfig.getProperty("canal.username");
			String password = CanalConfig.getProperty("canal.password");
			String batch_size = CanalConfig.getProperty("canal.batch_size");
			String filter_regex = CanalConfig.getProperty("canal.filter_regex");
			String isZookeeper = CanalConfig.getProperty("canal.zookeeper.enabled");
			MutiCanalFactory factory = new MutiCanalFactory(destinations, servers, username, password);
			if(!StringUtil.isEmpty(batch_size)||!StringUtil.isEmpty(filter_regex)||!StringUtil.isEmpty(isZookeeper)){
				if(!StringUtil.isEmpty(batch_size)){
					factory.setBatch_size(Integer.valueOf(batch_size));
				}
				if(!StringUtil.isEmpty(filter_regex)){
					factory.setFilter_regex(filter_regex);
				}
				if(!StringUtil.isEmpty(isZookeeper)){
					factory.setZookeeper(Boolean.valueOf(isZookeeper));
				}
			}
			factory.init();
			return factory;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
