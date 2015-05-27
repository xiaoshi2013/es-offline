package com.es.offline;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.es.offline.util.Tuple;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class IndicesTool {
	private String[] types;
	private String[] prefixs;
	
	public static final String INDEXES="indexes";

	public final ReentrantLock lock = new ReentrantLock();

	private static final Logger _LOG = Logger.getLogger(IndicesTool.class);

	private Kryo kryo;

	public IndicesTool(String[] types, String[] prefixs) throws FileNotFoundException {

		this.types = types;
		this.prefixs = prefixs;

		_LOG.info("prefixs " + Arrays.toString(prefixs));
		_LOG.info("types " + Arrays.toString(types));

		kryo = new Kryo();
		kryo.setReferences(false);

		//kryo.register(Tuple.class,new DeflateSerializer(kryo.getDefaultSerializer(Tuple.class)) );
		kryo.register(Tuple.class);

	}

	public void snapshot() throws Exception {
		
		Client client = ESClientUtil.getClient();
		
		String[] arr=EsAdminUtil.queryIndexNames(client, prefixs);
		
		File file=new File(INDEXES);
		if(file.exists()){
			file.delete();
		}
		file.createNewFile();
		FileUtils.writeLines(file, "UTF-8", Lists.newArrayList(arr));
		
		List<String> files=Lists.newArrayList();
		for (int i = 0; i < arr.length; i++) {
			File f = new File(arr[i]+".bin");
			if(f.exists()){
				System.out.println(f.getName()+" delete "+f.delete());
			}
			Output output = new Output(new FileOutputStream(f));
			//String query=Initialize.query;
			QueryBuilder qb = QueryBuilders.matchAllQuery();
			
			/*if(query!=null && !query.trim().equals("")){
				qb=QueryBuilders.queryString(query);
			}*/
			
			

			SearchRequestBuilder builder = client.prepareSearch(arr[i])
					.setQuery(qb)
					.setSearchType(SearchType.SCAN)
					.setScroll(new TimeValue(20000)).setSize(Initialize.size).setPreference("_local");

			/*if (types.length >= 1) {
				builder.setTypes(types);
			}*/
			_LOG.debug(builder);
			_LOG.debug(arr[i]);

			SearchResponse scrollResp = builder.execute().actionGet();
			_LOG.info("getTotalHits " + scrollResp.getHits().totalHits());

			long total=scrollResp.getHits().getTotalHits();
			long  start = System.currentTimeMillis();
			long  start1=start;
			while (true) {

				scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute()
						.actionGet();

				for (SearchHit hit : scrollResp.getHits()) {

					serializeObject(hit,output);

				}
				long now = System.currentTimeMillis();
				long interval = now - start;
				start = now;
				_LOG.debug("interval " + interval + " took " + scrollResp.getTookInMillis() + " hits "
						+ scrollResp.getHits().getHits().length);
				if (scrollResp.getHits().getHits().length == 0) {
					break;
				}

			}

			output.flush();
			output.close();
			files.add(f.getName());
			_LOG.info(f.getName()+" OK "+(System.currentTimeMillis()-start1)+"ms" +" "+total);
			
			
		}
	
		client.close();
		
		
		_LOG.info("over snapshot: "+files);

	}

	public void serializeObject(SearchHit hit,Output output) {
		Tuple tuple = new Tuple(hit.type(), hit.source());

		kryo.writeObject(output, tuple);

	}

	// http://zhangyongbo.iteye.com/blog/1913934 Kryo序列化框架开发测试
	@SuppressWarnings("unchecked")
	public void restore() throws IOException {

		
		Client client = ESClientUtil.createTargetClient();

		List<String> files=Lists.newArrayList();
		
		File indexes= new File(INDEXES);
		if(!indexes.exists()){
			throw new FileNotFoundException(indexes.getAbsolutePath()+" not exists");
		}
		List<String> list= FileUtils.readLines(indexes);
		
		
		for (int i = 0; i < list.size(); i++) {
			
			File f = new File(list.get(i)+".bin");
			if(!f.exists()){
				_LOG.error(f.getName() +" not exists");
				continue;
			}
			
			files.add(f.getName());
			long  start = System.currentTimeMillis();
			long  start1=start;
			
			Input input = new Input(new FileInputStream(f));
			int n = 0;
			Tuple<String, byte[]> tuple = null;

			BulkRequest bulk = new BulkRequest();

			while (!input.eof()) {
				tuple = kryo.readObject(input, Tuple.class);

				bulk.add(new IndexRequest(list.get(i), tuple.v1()).source(tuple.v2()).replicationType(ReplicationType.ASYNC)
						.consistencyLevel(WriteConsistencyLevel.ONE));
				if (bulk.numberOfActions() >= 5000) {
					BulkRequest bulktmp = bulk;
					bulk = new BulkRequest();
					BulkResponse res = client.execute(BulkAction.INSTANCE, bulktmp.replicationType(ReplicationType.ASYNC)
							.consistencyLevel(WriteConsistencyLevel.ONE)).actionGet();
					
					long now = System.currentTimeMillis();
					long interval = now - start;
					start = now;					
					_LOG.info(list.get(i) +" bulk took " + res.getTook()+" "+interval+" ms");
					if (res.hasFailures()) {
						_LOG.error(res.buildFailureMessage());
					}

				}
				n++;
			}

			if (bulk.numberOfActions() > 0) {
				BulkResponse res = client.execute(BulkAction.INSTANCE, bulk).actionGet();
				_LOG.info(list.get(i) +" bulk took " + res.getTook());
			}
			input.close();
			_LOG.info(list.get(i) +" --Total DocNums " + n+"  "+(System.currentTimeMillis()-start1)+"ms");
			
		}


		client.close();
		
		_LOG.info("over restore "+files);


	}

	public static void main(String[] args) throws Exception {

		String[] types = Initialize.types;
		String[] indexName = Initialize.prefix.split(",");
		
		if(args[0].equalsIgnoreCase("updateRrefresh")){
			System.out.println("-----updateRrefresh");
			EsAdminUtil.updateDynamicSettings();
		}
		else if(args[0].equalsIgnoreCase("test")){
			System.out.println("-----Indexing speed test");
			IndexTestTool.testStorage("file.bin", "gd-back");
		}
		else{
			
			IndicesTool scan = new IndicesTool(types, indexName);
			MappingUtil mappingUtil = new MappingUtil();
			try {

				if (args[0].equalsIgnoreCase("snapshot")) {
					System.out.println("-----snapshot");
					mappingUtil.getMapping();
					scan.snapshot();
					ESClientUtil.getClient().close();

				} else if (args[0].equalsIgnoreCase("restore")) {
					System.out.println("-----restore");
					mappingUtil.putMapping();
					scan.restore();
				}
				else{
					_LOG.error("error "+args[0]);
				}

			} catch (Exception e) {
				_LOG.error("error", e);
			}
			
			
		}

		

	}

}
