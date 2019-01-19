import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
//import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

public class ESTools {
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
			return client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("117.50.3.9"), 9300));
//			return client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("118.89.184.116"), 9300));
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
			throw new RuntimeException("获取ES连接出错");
		}
	}

}
