package weixinOneForOne;


import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
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
import wechatuploaddata.WechatUploadData;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
//import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

public class
ESTools {
	private ESTools() {
	}

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
			return client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("117.50.3.9"), 9300));
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
			throw new RuntimeException("获取ES连接出错");
		}
	}
}