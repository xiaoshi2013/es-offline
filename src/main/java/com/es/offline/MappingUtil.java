package com.es.offline;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.xcontent.XContentHelper;

import com.es.offline.util.Tuple;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class MappingUtil {

	private static final Logger _LOG = Logger.getLogger(MappingUtil.class);

	@SuppressWarnings("unchecked")
	public void getMapping() throws Exception {
	if(Initialize.prefix.trim().equals("")){
		throw new Exception("indexName is null");
	}
		String[] prefixs = Initialize.prefix.split(",");
		
		Client client = ESClientUtil.getClient();
		
		//Kryo kryo = new Kryo();
		//kryo.setReferences(false);

		//kryo.register(Tuple.class);
		List<String> files=Lists.newArrayList();

		String[] arr=EsAdminUtil.queryIndexNames(client, prefixs);

		
		for (int i = 0; i < arr.length; i++) {
			
			GetMappingsResponse res = client.admin().indices().prepareGetMappings(arr[i]).execute().actionGet();
			ImmutableOpenMap<String, MappingMetaData> mappings = (ImmutableOpenMap<String, MappingMetaData>) res.mappings()
					.values().toArray()[0];
			
			
			
			
			
			File f = new File(arr[i]+".mapping");
			System.out.println(f.getAbsolutePath()+" delete "+ f.delete());
			//Output output = new Output(new FileOutputStream(f));
			
			List<String> list=Lists.newArrayList();
			for (ObjectObjectCursor<String, MappingMetaData> cursor : mappings) {
				//_LOG.debug(cursor.value.type() + " " + cursor.value.sourceAsMap());
				//_LOG.debug(cursor.value.type() + " " +  cursor.value.source());
				list.add(cursor.value.source().string());
				//Tuple tuple = new Tuple(cursor.value.type(), cursor.value.sourceAsMap());
				//kryo.writeObject(output, tuple);
				

			}
			
			FileUtils.writeLines(f, "UTF-8", list);

			
			//output.flush();
			//output.close();
	
			_LOG.info(arr[i] + " snapshot mapping over");
			files.add(f.getName());
		}
		_LOG.info("over getMappings "+files);


	}

	public void putMapping() throws IOException {

		File indexes= new File(IndicesTool.INDEXES);
		if(!indexes.exists()){
			throw new FileNotFoundException(indexes.getAbsolutePath()+" not exists");
		}
		List<String> list= FileUtils.readLines(indexes);
		
		Client client = ESClientUtil.createTargetClient();

		/*Kryo kryo = new Kryo();
		kryo.setReferences(false);
		kryo.register(Tuple.class);
		List<String> files=Lists.newArrayList();

		for (int i = 0; i < list.size(); i++) {
			
			String index=list.get(i);
			File f = new File(index+".mapping");
			if(!f.exists()){
				_LOG.error(f.getName() +" not exists");
				continue;
			}
			files.add(f.getName());

			Input input = new Input(new FileInputStream(f));

			
			IndicesExistsResponse res = client.admin().indices().prepareExists(index).execute().actionGet();
			if (res.isExists()) {
			
				DeleteIndexResponse dres=  client.admin().indices().prepareDelete(index).execute().actionGet();
				 
				_LOG.info(" delete "+index+" "+dres.isAcknowledged());
				 
				 
				_LOG.info("create " + index);
				
			}
			
			 CreateIndexResponse  cres=	client.admin().indices().prepareCreate(index).execute().actionGet();
			
			_LOG.info("create index  "+index+" " +cres.isAcknowledged());
			while (!input.eof()) {
				Tuple<String, Map> tuple = kryo.readObject(input, Tuple.class);
				client.admin().indices().preparePutMapping(index).setType(tuple.v1()).setSource(tuple.v2()).execute()
						.actionGet();
				_LOG.debug(tuple);
				_LOG.info("putMapping " + index+" _type "+tuple.v1());

			}

			input.close();
		}*/
	
		List<String> files=Lists.newArrayList();
		for (int i = 0; i < list.size(); i++) {
			String index=list.get(i);
			File f = new File(index+".mapping");
			if(!f.exists()){
				_LOG.error(f.getName() +" not exists");
				continue;
			}
			files.add(f.getName());
			IndicesExistsResponse res = client.admin().indices().prepareExists(index).execute().actionGet();
			if (res.isExists()) {
				DeleteIndexResponse dres=  client.admin().indices().prepareDelete(index).execute().actionGet();
				_LOG.info(" delete "+index+" "+dres.isAcknowledged());
				
			}
			 CreateIndexResponse  cres=	client.admin().indices().prepareCreate(index).execute().actionGet();
			_LOG.info("create index  "+index+" " +cres.isAcknowledged());
			
			List<String> mappings=FileUtils.readLines(f, "UTF-8");
			
			for (String mapping : mappings) {
	            Map<String, Object> root = XContentHelper.convertToMap(mapping.getBytes("UTF-8"), true).v2();
	            String type=root.keySet().toArray(new String[1])[0];
	            
	            _LOG.info("type: "+type);
	            _LOG.info("mapping: "+root);
				client.admin().indices().preparePutMapping(index)
				.setType(type).setSource(root).execute()
				.actionGet();
				
				
			}

		}
		client.close();
		
		_LOG.info("over putMappings "+files);


	}

}
