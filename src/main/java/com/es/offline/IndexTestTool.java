package com.es.offline;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;

import com.es.offline.util.Tuple;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

public class IndexTestTool {

	private static final Logger _LOG = Logger.getLogger(IndexTestTool.class);

	private static Kryo kryo;

	public static void testStorage(String filename, String indexName) throws FileNotFoundException {

		File f = new File(filename);
		if (!f.exists()) {
			_LOG.error(f.getName() + " not exists");
			return;
		}
		Client client = ESClientUtil.createTargetClient();

		kryo = new Kryo();
		kryo.setReferences(false);
		kryo.register(Tuple.class);

		long start = System.currentTimeMillis();
		long start1 = start;

		Input input = new Input(new FileInputStream(f));
		int n = 0;
		Tuple<String, byte[]> tuple = null;
		final AtomicLong atom = new AtomicLong();

		BulkProcessor process = BulkProcessor.builder(client, new Listener() {

			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
				request.replicationType(ReplicationType.DEFAULT);
				atom.set(System.currentTimeMillis());
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
				// TODO Auto-generated method stub

			}

			@Override
			public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
				System.out.println("afterBulk " + response.getTook() + " " + (System.currentTimeMillis() - atom.get())
						+ "ms");

			}
		}).setBulkActions(5000).build();

		while (!input.eof()) {
			tuple = kryo.readObject(input, Tuple.class);
			process.add(new IndexRequest(indexName, tuple.v1()).source(tuple.v2()));
			n++;
		}

		process.flush();
		process.close();
		input.close();
		_LOG.info(indexName + " --Total DocNums " + n + "  " + (System.currentTimeMillis() - start1) + "ms");

		client.close();

		_LOG.info("over test ");

	}
}
