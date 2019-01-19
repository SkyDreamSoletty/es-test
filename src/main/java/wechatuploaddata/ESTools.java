package wechatuploaddata;


import com.unboundid.util.json.JSONObject;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
//import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

public class
ESTools {
	private ESTools() {}
	private static final TransportClient client = new PreBuiltTransportClient(Settings.builder()
            .put("cluster.name", "elasticsearch")
            .build());
	
//	private final static Settings settings = Settings.builder()
//			.put("request.headers.X-Found-Cluster", "elasticsearch")
//			.put("cluster.name", "elasticsearch")
//			.put("xpack.security.transport.ssl.enabled", false)
//			.put("xpack.security.user","elastic:changeme")
//			.put("client.transport.sniff", true)
//			.build();
//	private static final TransportClient client = new PreBuiltXPackTransportClient(settings);
//	private static final TransportClient client = new PreBuiltTransportClient(settings);
//			.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("115.28.85.102"), 9300));

	public static TransportClient getClient() {
		try {
			return client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("115.28.85.102"), 9300));
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
			throw new RuntimeException("获取ES连接出错");
		}
	}

	public static void batchByBulk(TransportClient client,List<Map<String,Object>> datas,String type){
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		IndexRequestBuilder indexRequestBuilder = null;
		try {
			for(Map<String,Object> data : datas ){
				indexRequestBuilder = client.prepareIndex("wechatdata", type);
				XContentBuilder json = XContentFactory.jsonBuilder()
						.startObject();
				for(String key : data.keySet()){
					json.field(key,data.get(key));
				}
				json.endObject();
				indexRequestBuilder.setSource(json);
				bulkRequestBuilder.add(indexRequestBuilder);
			}
			BulkResponse bulkResponse = bulkRequestBuilder.get();
			for(BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
				System.out.println("version: " + bulkItemResponse.getVersion());
			}
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("上传聚合数据出错");
		}finally {
			System.out.println("上传完成");
		}
	}

	public static String maxValue(TransportClient client,String index,String type,String field){
		SearchResponse response;
		SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
		MaxAggregationBuilder maxAggre = AggregationBuilders.max("agg").field(field);
		response = ps.addAggregation(maxAggre).get();
		Max agg = response.getAggregations().get("agg");
		String max = agg.getValueAsString();
		return max;
	}

	public static String minValue(TransportClient client,String index,String type,String field){
		SearchResponse response;
		SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
		MinAggregationBuilder minAggre = AggregationBuilders.min("agg").field(field);
		response = ps.addAggregation(minAggre).get();
		Min agg = response.getAggregations().get("agg");
		String min = agg.getValueAsString();
		return min;
	}

	public static void batchUploadBybulk( TransportClient client,String index,String type,String id,Map<String,String> data) throws IOException {
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, type)
				.setSource(XContentFactory.jsonBuilder()
				.startObject()
				.field("dataType", data.get("dataType"))
				.field("date", data.get("date"))
				.field("md5", data.get("md5"))
				.field("exposure", data.get("exposure"))
				.field("click_amount", data.get("click_amount"))
				.field("acv", data.get("acv"))
				.field("sfv", data.get("sfv"))
				.field("imgUrl", data.get("imgUrl"))
				.field("uploadDate", data.get("uploadDate"))
				.field("fileName", data.get("fileName"))
				.endObject());
		bulkRequestBuilder.add(indexRequestBuilder);
		bulkRequestBuilder.add(indexRequestBuilder);
		bulkRequestBuilder.add(indexRequestBuilder);
		BulkResponse bulkResponse = bulkRequestBuilder.get();

		for(BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
			System.out.println("version: " + bulkItemResponse.getVersion());
		}

		client.close();


	}

	public static void batchUploadBybulk( TransportClient client,String index,String type,String id,WechatUploadData data) throws IOException {
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		IndexRequestBuilder indexRequestBuilder = client.prepareIndex(index, type)
				.setSource(XContentFactory.jsonBuilder()
						.startObject()
						.field("dataType", data.getDataType())
						.field("date", data.getDate())
						.field("md5", data.getMd5())
						.field("exposure", data.getExposure())
						.field("click_amount", data.getClickAmount())
						.field("acv", data.getAcv())
						.field("sfv", data.getSfv())
						.field("imgUrl", data.getImgUrl())
						.field("uploadDate", data.getUploadDate())
						.field("fileName", data.getFileName())
						.endObject());
		bulkRequestBuilder.add(indexRequestBuilder);
		bulkRequestBuilder.add(indexRequestBuilder);
		bulkRequestBuilder.add(indexRequestBuilder);
		BulkResponse bulkResponse = bulkRequestBuilder.get();

		for(BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
			System.out.println("version: " + bulkItemResponse.getVersion());
		}

		client.close();


	}

}
