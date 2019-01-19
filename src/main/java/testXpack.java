import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import java.net.InetAddress;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

public class testXpack {

    @SuppressWarnings({ "unchecked", "resource" })
    public static void main(String[] args) throws Exception {

        String clusterName = "elasticsearch";
        // 先构建client
        Settings settings = Settings.builder()
                .put("client.transport.nodes_sampler_interval", "5s")
                .put("client.transport.sniff", false)
                .put("transport.tcp.compress", true)
                .put("cluster.name", clusterName)
                .put("xpack.security.transport.ssl.enabled", false)
                .put("request.headers.X-Found-Cluster", "${cluster.name}")
                .put("xpack.security.user", "transport_client_user:changeme")
                .build();
        TransportClient client = new PreBuiltXPackTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("115.28.85.102"), 9300));

        MatchQueryBuilder qb = matchQuery("id","12754564");

        SearchResponse response = client
                .prepareSearch()
                .setQuery(qb)
                .get();
        long totalHits = response.getHits().getTotalHits();
        System.out.println(totalHits);
        client.close();
    }

}
