package com.nascent.elasticsearch.indexroller;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class NewDocScanner {
	final int ID_SIZE=ExistChecker.ID_SIZE;
	final Scroll SCROLL = new Scroll(TimeValue.timeValueMinutes(1L));
	RestHighLevelClient client;
	ArrayBlockingQueue<String> writingQueue;
	
	public NewDocScanner(RestHighLevelClient c,ArrayBlockingQueue<String> q) {
		this.client=c;
		this.writingQueue=q;
	}
	
	/**
	 * 检查index内， 指定时间戳内是否有最新的数据？ 并用scroll把_id全部写入到BlockingQueue
	 */
	public Long checkInsert(String ingestIndex,String updateTsField,String tsFrom,String tsEnd) throws IOException, InterruptedException {
		SearchSourceBuilder query = new SearchSourceBuilder()
				.size(ID_SIZE)
				.fetchSource(false)//_id only!!!
				.query(QueryBuilders
						.rangeQuery(updateTsField)
						.from(tsFrom, true)// >= lower
						.to(tsEnd, false)//  <  upper
						);

		
		SearchRequest searchRequest = Requests.searchRequest(ingestIndex)
			.scroll(SCROLL)
			.source(query);

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT); 
		Long count = searchResponse.getHits().totalHits;
		System.out.println(ingestIndex+" found ["+count+"] new docs in timestamp ["+tsFrom+"]");
		
		String scrollId = searchResponse.getScrollId();
		SearchHit[] searchHits = searchResponse.getHits().getHits();
		while (searchHits != null && searchHits.length > 0) {
			extractIdFields(searchHits);
			
		    searchResponse = client.scroll(Requests
		    		.searchScrollRequest(scrollId)
		    		.scroll(SCROLL), RequestOptions.DEFAULT);
		    scrollId = searchResponse.getScrollId();
		    searchHits = searchResponse.getHits().getHits();//仅有数据时继续scroll
		}
		
		ClearScrollRequest clearScrollRequest = new ClearScrollRequest(); 
		clearScrollRequest.addScrollId(scrollId);
		client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
		//clearScrollResponse.isSucceeded();
		System.out.println("1. Scan  thread QUIT, to_check="+count);
		writingQueue.put(TimeStampReader.TS_PREFIX);//END SIGNAL!!!
		return count;
	}
	
	private void extractIdFields(SearchHit[] searchHits) throws InterruptedException {
		for(SearchHit hit :searchHits) {
			writingQueue.put(hit.getId());
		}
		//System.out.println("add a checking batch: "+searchHits.length);
	}
}
