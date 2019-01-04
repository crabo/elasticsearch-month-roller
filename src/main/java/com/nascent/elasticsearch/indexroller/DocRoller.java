package com.nascent.elasticsearch.indexroller;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class DocRoller {
	final int ID_SIZE=ExistChecker.ID_SIZE;
	RestHighLevelClient client;
	ArrayBlockingQueue<String> rollupQueue;
	
	public DocRoller(RestHighLevelClient c,ArrayBlockingQueue<String> q) {
		this.client=c;
		this.rollupQueue=q;
	}
	
	/**
	 * 从rollupQueue的id，提取newIndex的_souce， update合并到oldIndex。 最后从newIndex移除。
	 * @throws InterruptedException 
	 */
	public void rollup(String newIndex,String oldIndex){
		try {
			IdsQueryBuilder query = QueryBuilders.idsQuery();
			int i=0;
			String id;
			BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
			        (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
			BulkProcessor executor = BulkProcessor.builder(bulkConsumer, dummmyListener)
					.setBulkActions(2000)
					.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB))
					.build();
			
			while(!TimeStampReader.TS_PREFIX.equals(
					id=rollupQueue.take())) {//RECEIVE END SIGNAL?
				query.addIds(id);
				i++;
				
				if(i>=ID_SIZE) {//达到一个batch立即执行query
					execQuery(executor,newIndex,oldIndex,query);
					
					i=0;
					query = QueryBuilders.idsQuery();
				}
			}
			
			if(i>0) {//最后一个batch
				execQuery(executor,newIndex,oldIndex,query);
			}
			
			if(!executor.awaitClose(60, TimeUnit.SECONDS)) {
				System.out.println("---rollup _bulk requests failed!---");
			}
		} catch (Exception e) {
			System.out.println("error rolling up index:"+e.toString());
			e.printStackTrace();
		}
		System.out.println("\n3. Rollup thread QUIT");
	}
	
	/**
	 * 将newIndex的对应数据， update到oldIndex。 最后从newIndex清理删除。
	 * @param newIndex
	 * @param oldIndex
	 * @param q
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void execQuery(BulkProcessor executor,String newIndex,String oldIndex,IdsQueryBuilder q) throws IOException, InterruptedException {
		
		SearchSourceBuilder query = new SearchSourceBuilder()
				.size(ID_SIZE)
				.query(q);

		SearchRequest searchRequest = Requests.searchRequest(newIndex)
				.source(query);

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		SearchHits hits = searchResponse.getHits();
		if(hits.totalHits>0) {
			hittedDocs+=hits.totalHits;
			System.out.println("\t - moved ["+hittedDocs+"] docs to old index -");
			addBatchs(executor,oldIndex,hits.getHits());
		}
	}
	int hittedDocs=0;
	
	private void addBatchs(BulkProcessor executor,String updateIndex,SearchHit[] searchHits) throws InterruptedException, IOException {
		for(SearchHit hit :searchHits) {
			
			UpdateRequest upd=new UpdateRequest(updateIndex, hit.getType(), hit.getId())
					.doc(hit.getSourceAsMap());
			
			DeleteRequest del = new DeleteRequest(hit.getIndex(),hit.getType(),hit.getId());
			if(hit.field("_routing")!=null) {
				upd.routing(hit.field("_routing").getValue());
				del.routing(hit.field("_routing").getValue());
			}
			
			executor.add(upd);
			executor.add(del);
		}
		Thread.sleep(500);//避免batch添加速度太快
	}
	BulkProcessor.Listener dummmyListener = new BulkProcessor.Listener() {
		@Override
		public void beforeBulk(long executionId, BulkRequest request) {}
		@Override
		public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
			if(response.hasFailures())
				System.out.println("\t_bulk error: "+executionId +"\n"+response.buildFailureMessage());
			else if(executionId%20==0)
				System.out.println("\t_bulk success: "+executionId);
		}

		@Override
		public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
			System.out.println("\t_bulk error:");
			failure.printStackTrace();
		}
	};
}
