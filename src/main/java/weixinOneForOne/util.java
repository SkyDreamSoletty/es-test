package weixinOneForOne;

import com.alibaba.fastjson.JSONArray;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;

import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.*;

public class util {

    public static Integer countDocsForDateForIndex(TransportClient client, String index,String type,String bigin,String end){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        MatchAllQueryBuilder maqb = matchAllQuery();
        ps.setQuery(maqb).setSize(0);
        response = ps.get();
        Long count = response.getHits().getTotalHits();
//        SearchHit[] searchHits = response.getHits().getHits();
        return 0;
    }

    /**
     * 判断传入的index名称是否存在
     * @param index
     * @return
     */
    public static boolean indexExists(String index){
        IndicesExistsRequest request = new IndicesExistsRequest(index);
        IndicesExistsResponse response = ESTools.getClient().admin().indices().exists(request).actionGet();
        if (response.isExists()) {
            return true;
        }
        return false;
    }

    /**
     * 获取指定index中词条
     * @param client
     * @param index
     * @param type
     * @param size
     * @return
     */
    public static Map<?,?> getDataToCsv(TransportClient client, String index,String type,Integer size){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        MatchAllQueryBuilder maqb = matchAllQuery();
        ps.setQuery(maqb).setSize(size);
        response = ps.get();
        Long count = response.getHits().getTotalHits();
        return null;
    }

    /**
     * 获取指定index中词条数量
     * @param client
     * @param index
     * @param type
     * @return
     */
    public static Long countForIndexDocs(TransportClient client, String index,String type){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        MatchAllQueryBuilder maqb = matchAllQuery();
        ps.setQuery(maqb).setSize(0);
        response = ps.get();
        Long count = response.getHits().getTotalHits();
        return count;
    }


    /**
     * 通过传入参数，计算聚合结果的数量
     * @param client
     * @param index
     * @param type
     * @param aggeName
     * @param aggeField
     * @param fieldValue
     * @param rangeStart
     * @param rangEnd
     * @return
     */
    public static Long count(TransportClient client, String index, String type, String aggeName, String aggeField, String fieldValue, String field, String rangeStart, String rangEnd){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        if(aggeName.endsWith("Term")){
            QueryBuilder qb = matchQuery(field, fieldValue);
            ps.setQuery(qb);
        } else if(aggeName.endsWith("Range")){
            RangeQueryBuilder rqb = rangeQuery(field);
            if(rangeStart==null&&rangEnd!=null){
                rqb.to(rangEnd);
            }else if(rangEnd==null&&rangeStart!=null){
                rqb.from(rangeStart);
            }else if(rangeStart!=null&&rangEnd!=null){
                rqb.from(rangeStart).to(rangEnd);
            }
            ps.setQuery(rqb);
        }else{
            BoolQueryBuilder bqb = boolQuery();
            bqb.must(termQuery(field,fieldValue));
            RangeQueryBuilder rqb = rangeQuery(aggeField);
            if(rangeStart==null&&rangEnd!=null){
                rqb.to(rangEnd);
            }else if(rangEnd==null&&rangeStart!=null){
                rqb.from(rangeStart);
            }else if(rangeStart!=null&&rangEnd!=null){
                rqb.from(rangeStart).to(rangEnd);
            }
            bqb.must(rqb);
            ps.setQuery(bqb);
        }
        CardinalityAggregationBuilder agge = AggregationBuilders.cardinality(aggeName).field(aggeField).precisionThreshold(200000);
        ps.addAggregation(agge);
        response = ps.get();
        Cardinality agg = response.getAggregations().get(aggeName);
        Long cardinalty = agg.getValue();
        return cardinalty;

    }


    public static Terms batchQueryForUrl(TransportClient client, String index, String type, JSONArray ja, String boolField, String rangeField, String StartDate, String EndDate, Map<String,String> aggs, String groupName, String groupField, String sortName, String sort, Integer size){
//    public static Map<String,Aggregation> batchQueryForUrl(TransportClient client, String index, String type, JSONArray ja, String boolField, String rangeField, String StartDate, String EndDate, Map<String,String> aggs, String groupName, String groupField,String sortName, String sort, Integer size){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        BoolQueryBuilder bqb = boolQuery();
        for(Object key : ja){
            bqb.should().add(wildcardQuery(boolField,key.toString()));
        }
        RangeQueryBuilder rqb = rangeQuery(rangeField);
        if(StartDate==null&&EndDate!=null){
            rqb.to(EndDate);
        }else if(EndDate==null&&StartDate!=null){
            rqb.from(StartDate);
        }else if(StartDate!=null&&EndDate!=null){
            rqb.from(StartDate).to(EndDate);
        }
        bqb.filter(rqb);
        ps.setQuery(bqb);
//        ps.setPostFilter(rqb);
//        AggregationBuilder Aggre =
//                AggregationBuilders
//                        .dateRange("agg")
//                        .field("uploaddate")
//                        .format("yyyy-MM-dd")
//                        .addUnboundedTo("1950")    // from -infinity to 1950 (excluded)
//                        .addRange("2017-09-09", "2017-10-13");  // from 1950 to 1960 (excluded)
//                        .addUnboundedFrom("1960"); // from 1960 to +infinity
//        Aggre.subAggregation(AggregationBuilders.dateRange());
        AggregationBuilder Aggre = AggregationBuilders.terms(groupName).field(groupField).order(Terms.Order.term(false)).size(size);
        for(String key : aggs.keySet()){
            Aggre.subAggregation(AggregationBuilders.stats(key).field(aggs.get(key)));
//            StatsAggregationBuilder statsAggre = AggregationBuilders.stats(key).field(aggs.get(key));
//            ps.addAggregation(statsAggre);
//			Aggre.subAggregation(AggregationBuilders.sum(key).field(aggs.get(key)));
        }
//        Aggre.subAggregation(AggregationBuilders.sum(sortName).field(sort));
        response = ps.addAggregation(Aggre).get();
        Terms imgurl = response.getAggregations().get(groupName);
        return imgurl;
//        response = ps.get();
//        Map<String,Aggregation> agg = response.getAggregations().getAsMap();
//        return agg;
    }

//    clickamount:sum:39391.0
//    clickamount:avg:79.58
//    acv:sum:15308.0
//    acv:avg:30.93
//    exposure:sum:630415.0
//    exposure:avg:1273.57
//    sfv:sum:6329.0
//    sfv:avg:12.79

}
