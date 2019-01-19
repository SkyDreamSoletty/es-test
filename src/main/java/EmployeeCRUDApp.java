//import java.net.InetAddress;
//import java.util.Collections;
//
//import org.elasticsearch.action.delete.DeleteResponse;
//import org.elasticsearch.action.get.GetResponse;
//import org.elasticsearch.action.index.IndexResponse;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.action.update.UpdateResponse;
//import org.elasticsearch.client.Client;
//import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
//import org.elasticsearch.common.xcontent.XContentFactory;
//import org.elasticsearch.index.query.MatchQueryBuilder;
//import org.elasticsearch.index.search.MultiMatchQuery;
//import org.elasticsearch.transport.client.PreBuiltTransportClient;
//import org.elasticsearch.xpack.XPackClient;
//import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
//import org.elasticsearch.xpack.security.authc.support.SecuredString;
//import org.elasticsearch.xpack.security.client.SecurityClient;
//
//import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
//import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
//
///**
// * 员工增删改查的应用程序
// * @author Administrator
// *
// */
//public class EmployeeCRUDApp {
//
//	@SuppressWarnings({ "unchecked", "resource" })
//	public static void main(String[] args) throws Exception {
//		// 先构建client
//		Settings settings = Settings.builder()
//				.put("request.headers.X-Found-Cluster", "elasticsearch")
//				.put("cluster.name", "elasticsearch")
//				.put("xpack.security.transport.ssl.enabled", false)
//				.put("xpack.security.user","elastic:changeme")
//				.put("client.transport.sniff", true)
//				.build();
//		TransportClient client = new PreBuiltXPackTransportClient(settings)
//				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("115.28.85.102"), 9300));
//
//		MatchQueryBuilder qb = matchQuery("id","12754564");
////		XPackClient xpackClient = new XPackClient(client);
////		SecurityClient securityClient = xpackClient.security();
////		String token = basicAuthHeaderValue("elastic", new SecuredString("budong123".toCharArray()));
////		SearchResponse response = client.filterWithHeader(Collections.singletonMap("Authorization", token))
////				.prepareSearch()
////				.setQuery(qb)
////				.get();
//
//		SearchResponse response = client
//				.prepareSearch()
//				.setQuery(qb)
//				.get();
//		long totalHits = response.getHits().getTotalHits();
//		System.out.println(totalHits);
//
//		createEmployee(client);
////		getEmployee(client);
////		updateEmployee(client);
////		deleteEmployee(client);
//
//		client.close();
//	}
//
//	/**
//	 * 创建员工信息（创建一个document）
//	 * @param client
//	 */
//	private static void createEmployee(TransportClient client) throws Exception {
//		IndexResponse response = client.prepareIndex("company", "employee", "1")
//				.setSource(XContentFactory.jsonBuilder()
//						.startObject()
//							.field("name", "jack")
//							.field("age", 27)
//							.field("position", "technique")
//							.field("country", "china")
//							.field("join_date", "2017-01-01")
//							.field("salary", 10000)
//						.endObject())
//				.get();
//		System.out.println(response.getResult());
//	}
//
//	/**
//	 * 获取员工信息
//	 * @param client
//	 * @throws Exception
//	 */
//	private static void getEmployee(TransportClient client) throws Exception {
//		GetResponse response = client.prepareGet("search", "raw", "10262445").get();
//		System.out.println(response.getSourceAsString());
//	}
//
//	/**
//	 * 修改员工信息
//	 * @param client
//	 * @throws Exception
//	 */
//	private static void updateEmployee(TransportClient client) throws Exception {
//		UpdateResponse response = client.prepareUpdate("company", "employee", "1")
//				.setDoc(XContentFactory.jsonBuilder()
//							.startObject()
//								.field("position", "technique manager")
//							.endObject())
//				.get();
//		System.out.println(response.getResult());
// 	}
//
//	/**
//	 * 删除 员工信息
//	 * @param client
//	 * @throws Exception
//	 */
//	private static void deleteEmployee(TransportClient client) throws Exception {
//		DeleteResponse response = client.prepareDelete("company", "employee", "1").get();
//		System.out.println(response.getResult());
//	}
//
//}
