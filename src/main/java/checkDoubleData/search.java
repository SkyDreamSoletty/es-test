package checkDoubleData;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import weixinOneForOne.ESTools;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

public class search {
    public static void main(String[] args) {
        Map<String,Object> map = util.info(ESTools.getClient(),"wechatupload","data");
        SearchHit[] searchHits = (SearchHit[]) map.get("searchHits");
        for(int i = 0; i < searchHits.length; i++) {
            System.out.println(searchHits[i].getSourceAsString());
            JSONObject hits = JSONObject.parseObject(searchHits[i].getSourceAsString());
            System.out.println(hits.getString("clickexposure"));
        }
    }
}

class util{
    /**
     * 传入过滤范围参数，排序参数，分页参数，返回对应总数和结果集
     * @param index
     * @param type
     * @return
     */
    public static Map<String,Object> info(TransportClient client, String index, String type){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        RangeQueryBuilder rqb = rangeQuery("uploaddate").from("2017-11-09").to("2017-11-09");
        ps.setQuery(rqb);
        ps.addSort("exposure",SortOrder.DESC);
        ps.setFrom(0);
        ps.setSize(10);
        response = ps.get();
        long count = response.getHits().getTotalHits();
        SearchHit[] searchHits = response.getHits().getHits();
        Map<String,Object> map = new HashMap();
        map.put("count",count);
        map.put("searchHits",searchHits);
        return map;
    }

}
