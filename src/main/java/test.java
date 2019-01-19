
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
//import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
//import org.elasticsearch.search.builder.SearchSourceBuilder;
//import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
//import org.elasticsearch.search.sort.SortOrder;
//import org.elasticsearch.transport.client.PreBuiltTransportClient;

import static java.net.InetAddress.*;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * 员工搜索应用程序
 * @author Administrator
 *
 */
public class test {

    private static TransportClient c;

//    static {
//        try {
//            c = new PreBuiltTransportClient(
//                    Settings.builder()
//                            .put("cluster.name", "elasticsearch")
//                            .build())
//                    .addTransportAddress(new InetSocketTransportAddress(getByName("118.89.184.116"), 9300));
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//    }
    public static TransportClient getClient(){
        try {
            c = new PreBuiltXPackTransportClient(
                    Settings.builder()
//                            .put("request.headers.X-Found-Cluster", "elasticsearch")
                            .put("cluster.name", "elasticsearch")
//                            .put("xpack.security.transport.ssl.enabled", false)
//                            .put("xpack.security.user","elastic:changeme")
//                            .put("client.transport.sniff", true)
                            .build())
                    .addTransportAddress(new InetSocketTransportAddress(getByName("115.28.85.102"), 9300));
            return c;
        } catch (UnknownHostException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }


    @SuppressWarnings({ "unchecked", "resource" })
    public static void main(String[] args) throws Exception {
//        Settings settings = Settings.builder()
//                .put("cluster.name", "elasticsearch")
//                .build();
//        TransportClient client = new PreBuiltTransportClient(settings)
//                .addTransportAddress(new InetSocketTransportAddress(getByName("118.89.184.116"), 9300));
        TransportClient client = test.getClient();

//		prepareData(client);
//        executeSearch(client);
        search1(client);
//        client.close();
        search1(client);
//            b1(client);




    }
    public static void b1(TransportClient client){
        BulkRequestBuilder bulkRequest = client.prepareBulk();
            UpdateRequestBuilder updateRequestBuilder = null;
            try {
                updateRequestBuilder = client.prepareUpdate("search_raw", "raw", "11437534561")
                        .setDoc(XContentFactory.jsonBuilder()
                                .startObject()
                                .field("tag", 1)
//                                .field("update_at", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()))
                                .field("update_at",new Date())
                                .field("update_by", 24)
                                .endObject());
            } catch (IOException e) {
                e.printStackTrace();
            }
            bulkRequest.add(updateRequestBuilder);
        BulkResponse bulkResponse = bulkRequest.get();
        for(BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
            System.out.println("version: " + bulkItemResponse.getVersion());
        }
    }

    private static void search1(TransportClient client) {
        BoolQueryBuilder qb = boolQuery()
                  .must(termQuery("md5","07d62cea0f6b79b21e635f67f66e1ced"));
//                .must(termQuery("status","t"))
//                .must(termQuery("tag","1"));
        SearchResponse response = client.prepareSearch("search_raw")
                .setTypes("raw")
                .setQuery(qb)
//                .setFrom(0)
//                .setSize(30)
                .get();
//        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//        searchSourceBuilder.query(qb);
//        String[] fields = {"hostIP","pathFile"};
//        FetchSourceContext sourceContext = new FetchSourceContext();
//        searchSourceBuilder.fetchSource("code","title");
//        Object obj = response.toString();
//        System.out.println(obj.toString());

        SearchHit[] searchHits = response.getHits().getHits();
        System.out.println(searchHits[0].getSource());
        System.out.println(response.getHits().getTotalHits());
//        for(int i = 0; i < searchHits.length; i++) {
//            System.out.println(searchHits[i].getSourceAsString());
//        }
    }

    /**
     * 执行搜索操作
     * @param client
     */
    private static void executeSearch(TransportClient client) {
        QueryBuilder qb = prefixQuery(
                "gifurl",
                "http://img.soogif.com/"
        );


        SearchResponse response =
                client.prepareSearch("search_appl")
//                        .addSort("id", SortOrder.DESC)
                        .setTypes("appl")
                        .setQuery(qb)
//                .setQuery(QueryBuilders.matchQuery("position", "technique"))
//                .setPostFilter(QueryBuilders.rangeQuery("age").from(30).to(40))
//                .setFrom(0).setSize(3)
                        .get();

        SearchHit[] searchHits = response.getHits().getHits();
//        response.getHits().getTotalHits();
        System.out.println(response.getHits().getTotalHits());
        for(int i = 0; i < searchHits.length; i++) {
            System.out.println(searchHits[i].getSourceAsString());
        }
    }

    /**
     * 准备数据
     * @param client
     */
    private static void prepareData(TransportClient client) throws Exception {
        client.prepareIndex("company", "employee", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "jack")
                        .field("age", 27)
                        .field("position", "technique software")
                        .field("country", "china")
                        .field("join_date", "2017-01-01")
                        .field("salary", 10000)
                        .endObject())
                .get();

        client.prepareIndex("company", "employee", "2")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "marry")
                        .field("age", 35)
                        .field("position", "technique manager")
                        .field("country", "china")
                        .field("join_date", "2017-01-01")
                        .field("salary", 12000)
                        .endObject())
                .get();

        client.prepareIndex("company", "employee", "3")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "tom")
                        .field("age", 32)
                        .field("position", "senior technique software")
                        .field("country", "china")
                        .field("join_date", "2016-01-01")
                        .field("salary", 11000)
                        .endObject())
                .get();

        client.prepareIndex("company", "employee", "4")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "jen")
                        .field("age", 25)
                        .field("position", "junior finance")
                        .field("country", "usa")
                        .field("join_date", "2016-01-01")
                        .field("salary", 7000)
                        .endObject())
                .get();

        client.prepareIndex("company", "employee", "5")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "mike")
                        .field("age", 37)
                        .field("position", "finance manager")
                        .field("country", "usa")
                        .field("join_date", "2015-01-01")
                        .field("salary", 15000)
                        .endObject())
                .get();
    }

}
