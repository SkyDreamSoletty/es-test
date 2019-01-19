/*
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

*/
/**
 * Created by W8616 on 2017/7/4.
 *//*

public class test2q {
    public static void main(String[] args) throws UnknownHostException {

//        Settings settings = Settings.settingsBuilder()
//                .put("cluster.name", "my-application").build();
//        Client client = TransportClient.builder().settings(settings).build()
//                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.250"), 9300));
        executeSearch("my-application","192.168.1.250","http://img.soogif.com/","qiniulogs","qiniulogs",null,null);

    }

    */
/**
     *  返回匹配输入url的条数
     * @param clustreName  es服务器elasticsearch.yml中配置的集群名称
     * @param host  es服务器地址 :192.168.1.1
     * @param url 需要进行前缀匹配的url
     * @param index  es服务器上index名称
     * @param type  es服务器上的index下的type名称
     * @param startDate 查询的开始时间  2017-06-25T12:00:00Z   OR   2017-05-25
     * @param endDate 查询的结束时间
     * @throws UnknownHostException
     *//*

    private static void executeSearch(String clustreName,String host,String url,String index,String type,String startDate,String endDate) throws UnknownHostException {

        if(clustreName==null){
            clustreName = "elasticsearch";
        }
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", clustreName).build();
        Client client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), 9300));
        QueryBuilder qb = prefixQuery(
                "Url",
                url
        );
        RangeQueryBuilder qb2 = rangeQuery("@timestamp")
                .from(startDate).to(endDate);
        RangeQueryBuilder qb3 = rangeQuery("@timestamp")
                .from(startDate);
        RangeQueryBuilder qb4 = rangeQuery("@timestamp")
                .to(endDate);
//        2015-01-01T12：10：3​​0Z
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        if(startDate!=null&&endDate!=null){
            ps.setPostFilter(qb2);
        }else if(startDate!=null){
            ps.setPostFilter(qb3);
        }else if(endDate!=null){
            ps.setPostFilter(qb4);
        }
        response = ps.setQuery(qb)
//                        .setPostFilter(rangeQuery("@timestamp").from("2017-06-25T12:00:00Z").to("2017-07-05T12:00:00Z"))
                .get();

        SearchHit[] searchHits = response.getHits().getHits();
        System.out.println(response.getHits().getTotalHits());
        for(int i = 0; i < searchHits.length; i++) {
            System.out.println(searchHits[i].getSourceAsString());
        }
        client.close();
    }
}
*/
