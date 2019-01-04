package com.nascent.elasticsearch.indexroller;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class ExistChecker {

	public final static int ID_SIZE=1000;
	RestHighLevelClient client;
	ArrayBlockingQueue<String> readingQueue;
	ArrayBlockingQueue<String> rollupQueue;
	
	public ExistChecker(RestHighLevelClient c,ArrayBlockingQueue<String> r,ArrayBlockingQueue<String> w) {
		this.client=c;
		this.readingQueue=r;
		this.rollupQueue=w;
	}
	
	/**
	 * 提取readingQueue内的id， 并到查重index进行查重。 读取最长等待5分钟
	 * @param prevIndex 需进行查重的目标index
	 */
	public void checkExist(String prevIndex) throws InterruptedException, IOException {
		IdsQueryBuilder query = QueryBuilders.idsQuery();
		int i=0;
		String id;
		while(!TimeStampReader.TS_PREFIX.equals(
				id = readingQueue.take())) {//RECEIVE END SIGNAL!!!
			query.addIds(id);
			i++;
			
			if(i>=ID_SIZE) {//达到一个batch立即执行query
				execQuery(prevIndex,query);
				
				i=0;
				query = QueryBuilders.idsQuery();
			}
		}
		
		if(i>0) {//最后一个batch
			execQuery(prevIndex,query);
		}
		
		System.out.println("\n2. Check thread QUIT, to_move="+hittedDocs);
		rollupQueue.put(TimeStampReader.TS_PREFIX);//SEND END SIGNAL!!!
	}
	
	int missedDocs=0;
	int hittedDocs=0;
	/**
	 * 从指定index里返回 已存在 的ids[]， 并写入到rollupQueue
	 */
	private void execQuery(String prevIndex,IdsQueryBuilder q) throws IOException, InterruptedException {
		SearchSourceBuilder query = new SearchSourceBuilder()
				.size(ID_SIZE)
				.fetchSource(false)//_id only!!!
				.query(q);

		SearchRequest searchRequest = Requests.searchRequest(prevIndex)
				.source(query);

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		if(searchResponse.getHits().totalHits>0) {
			extractIdFields(searchResponse.getHits().getHits());
			
			hittedDocs+=searchResponse.getHits().totalHits;
			if(hittedDocs%ID_SIZE==0) {
				System.out.println("\tadded ["+hittedDocs+"] docs to rolling queue.");
			}
		}else {
			missedDocs +=ID_SIZE;
			if(missedDocs%(50*ID_SIZE)==0)
				System.out.println("\tskipped ["+missedDocs+"] not matched docs");
		}
	}
	
	private void extractIdFields(SearchHit[] searchHits) throws InterruptedException {
		for(SearchHit hit :searchHits) {
			rollupQueue.put(hit.getId());
		}
		//System.out.println("add a rollup batch: "+searchHits.length);
	}
}
