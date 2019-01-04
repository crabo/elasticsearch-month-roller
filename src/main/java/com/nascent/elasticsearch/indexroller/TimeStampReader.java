package com.nascent.elasticsearch.indexroller;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

/**
 * ys_send_{yyMM/3}
 * @author crabo
 */
public class TimeStampReader {
	public DateTimeFormatter TS_FORMAT=DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss+0800");
	
	RestHighLevelClient client;
	public static final String TS_PREFIX="rollup:";
	
	String configIndex;
	String indexPattern;
	DateTimeFormatter DATE_FORMAT;
	Integer interval=1;
	
	public TimeStampReader(RestHighLevelClient c,String config,String index) {
		this.client=c;
		this.configIndex=config;
		
		int i = index.indexOf("{");
		if(i>0) {//ys_send_{yyMM/3}
			String datePattern = index.substring(i+1,index.length()-1);
			int j = datePattern.indexOf("/");
			if(j>0) {
				interval=Integer.valueOf(datePattern.substring(j+1));
				
				datePattern=datePattern.substring(0,j);
			}
			
			this.indexPattern = index.substring(0,i);
			this.DATE_FORMAT = DateTimeFormatter.ofPattern(datePattern,Locale.CHINA);
		}
	}
	//[{"_index":".kibana","_type":"doc","_id":"rollup:ys_send-%%","_score":1.0,
	//"_source":{"type":"rollup","updated_at":"2018-06-14T03:02:06.678Z","config":{"buildNum":16627,"defaultIndex":"51ce89a0-6f7f-11e8-be9f-f149d4fb20ad"}}}
	public String queryTs(){
		try {
			SearchSourceBuilder query = new SearchSourceBuilder()
					.query(QueryBuilders.idsQuery().addIds(TS_PREFIX+indexPattern));
	
			SearchResponse searchResponse = client.search(
						Requests.searchRequest(configIndex)
								.source(query), RequestOptions.DEFAULT);
			
			SearchHits hits = searchResponse.getHits();
			if(hits.totalHits>0) {
				return (String)hits.getHits()[0].getSourceAsMap().getOrDefault("updated_at", null);
			}else {
				System.out.println("---No timestamp store in '"+configIndex+"', _id = ["+TS_PREFIX+indexPattern+"]---");
				String tsFrom = TS_FORMAT.format(LocalDateTime.now().minusMonths(1));
				updateTs(tsFrom);
				return tsFrom;
			}
		} catch (Exception e) {
			throw new RuntimeException("unable to read timestamp from index: "+configIndex,e);
		}
	}
	
	public void updateTs(String tsFrom) throws IOException {
		UpdateRequest upd=new UpdateRequest(configIndex, "doc", TS_PREFIX+indexPattern)
				.doc(new HashMap<String,String>(){{
					put("type","rollup");
					put("updated_at",tsFrom);
				}})
				.docAsUpsert(true)
				.retryOnConflict(3);
		client.update(upd, RequestOptions.DEFAULT);
		System.out.println("update timestamp=["+tsFrom+"] in '"+configIndex+"', _id = ["+TS_PREFIX+indexPattern+"]");
	}
	
	public String getCurrentIndex() {
		return indexPattern + LocalDateTime.now().format(DATE_FORMAT);
	}
	public String getPrevIndex() {
		String datestr = DATE_FORMAT.toString();
		if(datestr.indexOf("DayOfMonth")>0) {
			return indexPattern + LocalDateTime.now().minusDays(interval).format(DATE_FORMAT);
		}else if(datestr.indexOf("MonthOfYear")>0) {
			return indexPattern + LocalDateTime.now().minusMonths(interval).format(DATE_FORMAT);
		}else if(datestr.indexOf("Year")>0) {
			return indexPattern + LocalDateTime.now().minusYears(interval).format(DATE_FORMAT);
		}
		throw new IllegalArgumentException("invalid index pattern dateformat: "+indexPattern+datestr);
	}
}
