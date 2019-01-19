import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;


public class ElasticSearchDataImport {
//	private static String host = ElasticSearchConfig.get("elasticSearch.host");
//	private static int port = Integer.parseInt(ElasticSearchConfig.get("elasticSearch.port"));
//	private static String clusterName = ElasticSearchConfig.get("elasticSearch.cluster.name");

	private static Logger logger = Logger.getLogger(String.valueOf(ElasticSearchDataImport.class));
	private static TransportClient client  = null;

	private static TransportClient init(){
		if(client == null){
			try {
//				client = new PreBuiltTransportClient(Settings.builder().put("cluster.name", clusterName).build()).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), Integer.valueOf(port)));
				client = new PreBuiltTransportClient(Settings.builder().put("cluster.name", "elasticsearch").build()).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("117.50.3.9"), Integer.valueOf(9300)));
				return client;
			} catch (UnknownHostException e) {
				logger.info("----------------------初始化Client异常----------------------");
				e.printStackTrace();
			}
		}
		return client;
	}

//	public static void indexStructure() {
//		init();
//		logger.info("----------------------创建索引结构----------------------");
//		XContentBuilder builder;
//		try {
//			builder = XContentFactory
//					.jsonBuilder()
//					.startObject()
//						.startObject("alibaichuan")
//							.startObject("properties")
//								.startObject("_id")
//									.field("type", "integer")
//									.field("store", "yes")
//									.field("index", "analyzed")
//								.endObject()
//								.startObject("item_title")
//									.field("type", "string")
//									.field("store", "yes").field("analyzer", "hanlp-index")
//									.field("index", "analyzed")
//								.endObject()
//								.startObject("mmb_sort")
//									.field("type", "string")
//									.field("store", "yes")
//									.field("index", "analyzed")
//								.endObject()
//								.startObject("updated_at")
//									.field("type", "date")
//									.field("store", "yes")
//								.endObject()
//							.endObject()
//						.endObject()
//					.endObject();
//			PutMappingRequest mapping = Requests.putMappingRequest("alibaichuan")
//					.type("tbk_item").source(builder);
//			client.admin().indices().putMapping(mapping).actionGet();
//			logger.error("----------------------创建索引结构成功----------------------");
//		} catch (IOException e) {
//			logger.error("----------------------创建索引结构异常----------------------");
//			e.printStackTrace();
//		}
//	}
	
	public static void fullIndex(){
		
	}
	
	public static void incrementIndex(){
		
	}
	
	public static void main(String[] args) {
		init();
//		indexStructure();
	}
}
