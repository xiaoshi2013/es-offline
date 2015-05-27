package com.es.offline;

import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ESClientUtil {

	private static Logger _LOG = Logger.getLogger(ESClientUtil.class);

	private static TransportClient client;

	private static final ReentrantLock lock = new ReentrantLock();
	
	//private static long end=System.currentTimeMillis();
	
	//private static long interval=300*1000;
	

/*	static {
		try {
			initClient();
		} catch (Exception e) {
			e.printStackTrace();
			
			_LOG.error("pad-error",e);
		}
	}
*/
	public static void initClient() throws Exception {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("client.transport.ping_timeout", Initialize.TIMEOUT)
				.put("client.transport.sniff", true)
				.put("network.tcp.reuse_address", true)
				.put("cluster.name", Initialize.esname).build();

		TransportClient client = new TransportClient(settings);

		String[] eshost = Initialize.eshost.split(",");
		//String[] esport = Initialize.esport.split(",");

		for (int i = 0; i < eshost.length; i++) {
			client.addTransportAddress(new InetSocketTransportAddress(
					eshost[i], 9300));
		}

		ESClientUtil.client = client;
		_LOG.info("client "+ Initialize.eshost+" "+Initialize.esport+" "+Initialize.esname);

	}

	public static TransportClient getClient() throws Exception {
		lock.lock();
	

		try {
			
			if (client == null ) {
				//close();
				initClient();
				
			}
			return client;
		} finally {
			lock.unlock();
		}

	}
	
	public static Client createTargetClient(){
		
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("client.transport.ping_timeout", Initialize.TIMEOUT)
				.put("client.transport.sniff", true)
				.put("cluster.name", Initialize.estargetname).build();

		TransportClient client = new TransportClient(settings);

		_LOG.info("---"+Initialize.estarget);
		_LOG.info("---"+Initialize.estargetname);
		
		String[] eshost = Initialize.estarget.split(",");
		//String[] esport = Initialize.estargetport.split(",");

		
		for (int i = 0; i < eshost.length; i++) {
			client.addTransportAddress(new InetSocketTransportAddress(
					eshost[i], 9300));
		}
		
		return client;
		
	}

	public static void close() {
		lock.lock();
		try {
			if(client!=null){
				client.close();
			}
			_LOG.debug("ES client closed ");
			
		} catch (Exception e) {
			_LOG.error("pad-error",e);
		}
		finally{
			lock.unlock();
		}
		
	}
	
	public static void main(String[] args) {
		ESClientUtil.createTargetClient();
		System.out.println("ok");
		
		/*try {
			
			Thread t=new Thread(new Runnable() {
				
				@Override
				public void run() {
					 try {
						 Client client=ESClientUtil.getClient();
						 for (int i = 0; i < 10; i++) {
								CountResponse  res=client.prepareCount("topology-2014.08").execute().actionGet();
								System.out.println(res.getCount());
								TimeUnit.SECONDS.sleep(7);

						}
					} catch (ElasticsearchException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					
				}
			});
			t.start();
			
			
			 
			 
			for (int i = 0; i < 10; i++) {
				ESClientUtil.getClient();
				
				TimeUnit.SECONDS.sleep(15);
				

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

}
