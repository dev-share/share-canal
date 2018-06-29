package com.devzy.share.canal;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;
import com.devzy.share.util.DateUtil;
/**
 * <pre>
 * 描述:  Canal服务(MySQL数据库监控)
 * @author yi.zhang
 * 时间:  2017年6月1日 上午10:09:03
 * @since 1.0
 * </pre>
 */
public class CanalFactory {
	private static Logger logger = LogManager.getLogger(CanalFactory.class);
	private static CanalConnector connector;
	private static int BATCH_SIZE = 1000;
	/**
	 * 监控过滤规则(默认所有操作:.*\\..*)
	 * EX:
	 * 1.库db1下所有表:db1\\..*
	 * 2.库db1/库db2下所有表:db1\\..*,db2\\..*
	 * 3.库db1下table1表以及库db2下table2表:db1.table1,db2.table2
	 * 4.以name1开头以及包含name2的所有库表:.*\\.name1.*,.*\\.*.name2.*
	 */
	private static String CANAL_FILTER_REGEX = ".*\\..*";
	
	private String destination;
	private String servers;
	private String username;
	private String password;
	private String filter_regex;
	private boolean isZookeeper;
	private int batch_size;
	
	public CanalFactory() {
		super();
	}
	public CanalFactory(String destination, String servers, String username, String password) {
		super();
		this.destination = destination;
		this.servers = servers;
		this.username = username;
		this.password = password;
		this.filter_regex = CANAL_FILTER_REGEX;
		this.isZookeeper = false;
		this.batch_size = BATCH_SIZE;
	}
	public CanalFactory(String destination, String servers, String username, String password,boolean isZookeeper) {
		super();
		this.destination = destination;
		this.servers = servers;
		this.username = username;
		this.password = password;
		this.isZookeeper = isZookeeper;
		this.filter_regex = CANAL_FILTER_REGEX;
		this.batch_size = BATCH_SIZE;
	}
	public CanalFactory(String destination, String servers, String username, String password, boolean isZookeeper,String filter_regex) {
		super();
		this.destination = destination;
		this.servers = servers;
		this.username = username;
		this.password = password;
		this.filter_regex = filter_regex;
		this.isZookeeper = isZookeeper;
		this.batch_size = BATCH_SIZE;
	}
	public CanalFactory(String destination, String servers, String username, String password, boolean isZookeeper, String filter_regex,int batch_size) {
		super();
		this.destination = destination;
		this.servers = servers;
		this.username = username;
		this.password = password;
		this.filter_regex = filter_regex;
		this.isZookeeper = isZookeeper;
		this.batch_size = batch_size;
	}
	public String getDestination() {
		return destination;
	}
	public void setDestination(String destination) {
		this.destination = destination;
	}
	public String getServers() {
		return servers;
	}
	public void setServers(String servers) {
		this.servers = servers;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getFilter_regex() {
		return filter_regex;
	}
	public void setFilter_regex(String filter_regex) {
		this.filter_regex = filter_regex;
	}
	public boolean isZookeeper() {
		return isZookeeper;
	}
	public void setZookeeper(boolean isZookeeper) {
		this.isZookeeper = isZookeeper;
	}
	public int getBatch_size() {
		return batch_size;
	}
	public void setBatch_size(int batch_size) {
		this.batch_size = batch_size;
	}
	/**
	 * 描述:  Canal服务配置
	 * @author yi.zhang
	 * 时间:  2017年4月19日 上午10:38:42
	 */
	public void init(){
		try {
			if(isZookeeper){
				connector = CanalConnectors.newClusterConnector(servers, destination, username, password);
			}else{
				List<SocketAddress> addresses = new ArrayList<SocketAddress>();
				for(String address : servers.split(",")){
					String[] ips = address.split(":");
					String ip = ips[0];
					int port=11111;
					if(ips.length>1){
						port = Integer.valueOf(ips[1]);
					}
					addresses.add(new InetSocketAddress(ip, port));
				}
				connector = CanalConnectors.newClusterConnector(addresses, destination, username, password);
			}
			connector.connect();
			connector.subscribe(CANAL_FILTER_REGEX);
			connector.rollback();
		} catch (Exception e) {
			logger.error("-----Canal Config init Error-----", e);
		}
	}
	/**
	 * 关闭服务
	 */
	public static void close(){
		if(connector!=null){
			connector.disconnect();
		}
	}
	/**
	 * 提交数据
	 * @param batchId 批量ID
	 */
	public static void ack(long batchId){
		connector.ack(batchId);
	}
	/**
	 * 回滚数据
	 * @param batchId 批量ID
	 */
	public static void rollback(long batchId){
		connector.rollback(batchId);
	}
	/**
	 * 描述:  监控数据
	 * @author yi.zhang
	 * 时间:  2017年6月1日 上午10:10:52
	 * @return 返回监控数据
	 */
	public List<MonitorInfo> execute(){
		try {
			if(connector==null){
				init();
			}
			List<MonitorInfo> monitors = new ArrayList<MonitorInfo>();
			Message message = connector.getWithoutAck(BATCH_SIZE); // 获取指定数量的数据
			long batchId = message.getId();
			List<Entry> list = message.getEntries();
			if(list!=null&&list.size()>0){
				for (Entry entry : list) {
					if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
			            continue;
			        }
			        RowChange event = null;
			        try {
			        	event = RowChange.parseFrom(entry.getStoreValue());
			        } catch (Exception e) {
			            throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
			        }
			        String schema = entry.getHeader().getSchemaName();
			        String table = entry.getHeader().getTableName();
			        String type = event.hasEventType()?event.getEventType().name():null;
			        String sql = event.getSql();
			        System.out.println("-----{schema:"+schema+",table:"+table+",type:"+type+",sql:"+sql+"}");
			        MonitorInfo monitor = new MonitorInfo();
			        monitor.setSchema(schema);
			        monitor.setTable(table);
			        monitor.setType(type);
			        monitor.setSql(sql);
			        List<MonitorInfo.RowInfo> rows = monitor.getRows();
			        for (RowData rowData : event.getRowDatasList()) {
			        	MonitorInfo.RowInfo row = monitor.new RowInfo();
			        	JSONObject before = row.getBefore();
			        	JSONObject after = row.getAfter();
			        	JSONObject change = row.getChange();
			        	List<Column> cbefores = rowData.getBeforeColumnsList();
			        	List<Column> cafters = rowData.getAfterColumnsList();
			            for (Column column : cbefores) {
			            	String key = column.getName();
			            	Object value = column.getValue();
			            	String ctype = column.getMysqlType().toLowerCase();
			            	if(ctype.contains("int")){
			            		if(ctype.contains("bigint")){
			            			value = Long.valueOf(column.getValue());
			            		}else{
			            			value = Integer.valueOf(column.getValue());
			            		}
			            	}
			            	if(ctype.contains("decimal")||ctype.contains("numeric")||ctype.contains("double")||ctype.contains("float")){
			            		value = Double.valueOf(column.getValue());
			            	}
			            	if(ctype.contains("timestamp")||ctype.contains("date")){
			            		if(ctype.contains("timestamp")){
			            			value = DateUtil.formatDateTime(column.getValue());
			            		}else{
			            			value = DateUtil.formatDate(column.getValue());
			            		}
			            	}
			            	boolean update = column.getUpdated();
			            	before.put(key, value);
			            	if(update){
			            		change.put(key, value);
			            	}
			                System.out.println("--"+type+"--before----{"+key+ ": " + value + ",update: " + update+"}");
			            }
			            for (Column column : cafters) {
			            	String key = column.getName();
			            	Object value = column.getValue();
			            	String ctype = column.getMysqlType().toLowerCase();
			            	if(ctype.contains("int")){
			            		if(ctype.contains("bigint")){
			            			value = Long.valueOf(column.getValue());
			            		}else{
			            			value = Integer.valueOf(column.getValue());
			            		}
			            	}
			            	if(ctype.contains("decimal")||ctype.contains("numeric")||ctype.contains("double")||ctype.contains("float")){
			            		value = Double.valueOf(column.getValue());
			            	}
			            	if(ctype.contains("timestamp")||ctype.contains("date")){
			            		if(ctype.contains("timestamp")){
			            			value = DateUtil.formatDateTime(column.getValue());
			            		}else{
			            			value = DateUtil.formatDate(column.getValue());
			            		}
			            	}
			            	boolean update = column.getUpdated();
			            	after.put(key, value);
			            	if(update){
			            		change.put(key, value);
			            	}
			                System.out.println("--"+type+"--after----{"+key+ ": " + value + ",update: " + update+"}");
			            }
			            rows.add(row);
			        }
			        monitors.add(monitor);
			}
			}
			ack(batchId);
			return monitors;
		}catch (Exception e) {
			logger.error("--Canal监控失败!",e);
		}
		return null;
	}
}