package com.nascent.elasticsearch.indexroller;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * 将[tsFrom , now-10m]之间的新增数据，判断是否需要合并到to索引
 * 自动检测在同时在from和to存在的记录；执行从update合并到to，并从from删除。
 *
 */
public class App 
{
	/**
	 * java -Xmx1g -Dfrom=ys_1803 -Dto=ys_1800 -Dts=ModifiedOn
	 * @param args
	 */
    public static void main(String[] args )
    {
    	if(args.length==0) {
	    	System.out.println("usage: java -Xmx1g http://localhost:8200 -Dfrom=ys_1803 -Dto=ys_1800 -Dts=ModifiedOn -Dm=10"
	    			+"\n - host,命令行第一个参数： http://user:pwd@localhost:9200"
	    			+"\n - from,to 索引名称（可选） \n - index 索引[年月日格式]分库格式(必输) ys_send_{yyyyMM/2}"
	    			+"\n - ts 数据更新时间戳字段(必输)， 用于从from筛选指定返回增量数据"
	    			+"\n - m  延迟同步时间(可选)，避免主机之间时间不同，数据入库时间戳不一致");
    	}
    	
    	//-------------------------任务参数提取--------------
    	String indexPattern = System.getProperty("index",args.length>1?args[1]:null);
    	String indexTsField = System.getProperty("ts");
		if(indexPattern==null || indexTsField==null) {
			throw new IllegalArgumentException("you need to provide 'index' and 'ts' args!");
		}
		
    	RestHighLevelClient client = getClient(args[0]);
    	
    	TimeStampReader tsReader = new TimeStampReader(client,System.getProperty("config",".kibana"),indexPattern);
    	String fromIndex = System.getProperty("from",tsReader.getCurrentIndex());
    	String toIndex = System.getProperty("to",tsReader.getPrevIndex());
    	String minutesAgo = System.getProperty("m","10");
    	
    	//从上次ts，到当前时间-10m  传入参数m
    	String tsFrom = tsReader.queryTs();
    	String tsEnd = LocalDateTime.now()
    						.minusMinutes(Integer.valueOf(minutesAgo))
    						.withSecond(0)
    						.format(tsReader.TS_FORMAT);
    	System.out.println(String.format("rollup from '%s' to '%s', %s=[%s - %s]",
    			fromIndex,toIndex,
    			indexTsField,tsFrom,tsEnd));
    	
		//-------------------------线程任务启动--------------
    	ArrayBlockingQueue<String> tocheckQueue=new ArrayBlockingQueue<String>(10 * ExistChecker.ID_SIZE);
    	ArrayBlockingQueue<String> rollupQueue=new ArrayBlockingQueue<String>(5 * ExistChecker.ID_SIZE);
    	
    	//同时3个线程并行，提高处理速度。
    	ExecutorService executor = Executors.newFixedThreadPool(3);
    	executor.submit(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				//1.扫描最近的新增数据， 提取id并写入到tocheckQueue队列
				new NewDocScanner(client,tocheckQueue)
						.checkInsert(fromIndex, indexTsField, tsFrom, tsEnd);
				return null;
			}
    	});
    	
    	executor.submit(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				//2.逐批次判断tocheckQueue队列是否已存在于toIndex， 存在则id写入到rollupQueue
				new ExistChecker(client,tocheckQueue,rollupQueue)
						.checkExist(toIndex);
				return null;
			}
    	});
    	
    	executor.submit(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				//3.提取rollupQueue中的_source数据，并bulk迁移到toIndex。最后从fromIndex清理
				new DocRoller(client,rollupQueue)
						.rollup(fromIndex,toIndex);
				return null;
			}
    	});
    	
    	//-------------------------任务结束等待--------------
    	executor.shutdown();
    	try {
    		//4. 任务正常结束?更新时间戳
			if(executor.awaitTermination(1, TimeUnit.HOURS)) {
				tsReader.updateTs(tsEnd);
			}
			client.close();
			System.out.println("--- JOB FINISHED ---");
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
    }
    

	private static RestHighLevelClient getClient(String host) {
		int auth = host.indexOf("@");
		if(auth>0) {
			String authPwd =host.substring(host.indexOf("://")+3, auth);
			host = host.replace(authPwd+"@", "");
			
			//格式为  用户名：密码
    		byte[] credentials = Base64.encodeBase64(authPwd.getBytes(StandardCharsets.UTF_8));
			return new RestHighLevelClient(
	    	        RestClient.builder(HttpHost.create(host))
	    	        .setDefaultHeaders(new Header[] {
	    	        		new BasicHeader("Authorization","Basic " 
	    	    				+ new String(credentials, StandardCharsets.UTF_8))
    	        		}));
		}else
			return new RestHighLevelClient(
	    	        RestClient.builder(HttpHost.create(host)));
	}
}
