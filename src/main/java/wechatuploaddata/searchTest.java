package wechatuploaddata;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.*;

public class searchTest {
    private static final TransportClient client = ESTools.getClient();

    public static void main(String[] args) {
//        Arith.div(3,10,2);
//        System.out.println(Arith.div(10,0,4));

//        String date = ESTools.maxValue(ESTools.getClient(),"wechatupload","data","uploaddate");
//        String date2 = ESTools.minValue(ESTools.getClient(),"wechatupload","data","uploaddate");
//        query("wechatupload","data","imgurl","http://img.soogif.com/lNs2BFwcbfkfCTjzugNhkq8ztwZYpkZw.gif");
        Map<String,String> map = new HashMap<String,String>();
        map.put("exposureStats","exposure");
        map.put("ca","clickamount");
        map.put("acvStats","acv");
        map.put("sfvStats","sfv");
        List<String> keys = new ArrayList<String>();
        keys.add("exposure");
        keys.add("clickamount");
        keys.add("acv");
        keys.add("sfv");
//        Map<String,Aggregation> aggs = statsValue("wechatupload","data",map);
//        System.out.println(date);
//        System.out.println(date2);
//        for(String key : aggs.keySet()){
//            Stats agg = (Stats) aggs.get(key);
//            System.out.println(key+" : "+agg.getMax()+" : "+agg.getAvg());
//        }
//        Map<String,Object> map = infoForDate("wechatupload","data","uploaddate","2017-08-10","2017-08-11","exposure",SortOrder.DESC,0,50);
//        SearchHit[] searchHits = (SearchHit[]) map.get("searchHits");
//        for(int i = 0; i < searchHits.length; i++) {
//            String str = searchHits[i].getSourceAsString();
//            System.out.println(str);
//        }

//        infoForGroupImgUrlAndDate("wechatupload","data","uploaddate","2017-08-10","2017-08-11",map,"group_imgurl","imgurl.keyword",10 );
//        String date = maxValue(client,"wechatupload","data","uploaddate","http://img.soogif.com/lNs2BFwcbfkfCTjzugNhkq8ztwZYpkZw.gif");
//        System.out.println(date);

//        Map<String,Aggregation> aggs = statsValueForDateAndUrl("wechatupload","data",map,"uploaddate","2017-08-13","2017-08-20","http://img.soogif.com/A0iPgCXEQSUsoW0kg9ynhOqLsVAhQsaz.gif");
//        Map<String,Aggregation> aggs = statsValueForDate("wechatupload","data",map,"imgurl","uploaddate","2017-08-03","2017-08-10","http://img.soogif.com/A0iPgCXEQSUsoW0kg9ynhOqLsVAhQsaz.gif");
//        Map<String,Aggregation> aggs = avgValue(client,"wechatupload","data","imgurl","uploaddate","2017-08-04","2017-08-10",map,"http://img.soogif.com/A0iPgCXEQSUsoW0kg9ynhOqLsVAhQsaz.gif");
//        for(String key : aggs.keySet()){
//            Avg agg = (Avg) aggs.get(key);
//            System.out.println(key+" : "+agg.getValueAsString());
//        }
        Terms uploadDate = sumValueForDateAndUrl(client,"wechatupload","data",map,"groupUploadDate","uploaddate","imgurl","uploaddate","2017-08-04","2017-08-10","http://img.soogif.com/A0iPgCXEQSUsoW0kg9ynhOqLsVAhQsaz.gif","",true);
//        for (Terms.Bucket entry : imgurl.getBuckets()) {
//            System.out.println(entry.getKeyAsString());
//            for (String key : map.keySet()){
//                Sum acv_stats = entry.getAggregations().get(key);
//                System.out.println(map.get(key)+" : "+acv_stats.getValue());
//            }
//        }
//        Terms uploadDate = sumValueForDateAndUrl("wechatupload","data",map,"groupUploadDate","uploaddate","imgurl.keyword","uploaddate","2017-08-11","2017-08-15","http://img.soogif.com/00Hpam4DgCzv6goBtbyfYwXOCrDFqOKY.jpeg",null,false);
        for (Terms.Bucket entry : uploadDate.getBuckets()) {
            System.out.println(entry.getKeyAsString());
            for (String key : map.keySet()){
                Sum acv_stats = entry.getAggregations().get(key);
                System.out.println(map.get(key)+" : "+acv_stats.getValue());
            }
        }

//        String index = ("2017-07-16"+"2017-08-20").replaceAll("-","");
//        String type = "data";
//        long t = System.currentTimeMillis();
//        Terms imgurl = infoForGroupImgUrlAndDate(index,type,"uploaddate","2017-07-16","2017-08-20",map,"groupByImgUrl","imgurl.keyword",150);
//        long t2 = System.currentTimeMillis();
//        System.out.println("查询数据耗时："+(t2-t));
//        int n = 0;
//        List<Map<String,Object>> datas = new ArrayList<Map<String, Object>>();
//        for (Terms.Bucket entry : imgurl.getBuckets()) {
//            Map<String,Object> wechatData = new HashMap<String, Object>();
//            n++;
//            wechatData.put("imgUrl",entry.getKeyAsString());
//            for(String key:map.keySet()) {
//                Sum sum = entry.getAggregations().get(key);
//                wechatData.put(map.get(key), sum.getValueAsString());
//            }
//            datas.add(wechatData);
//            System.out.println(JSONObject.toJSONString(wechatData));
//            JSONObject.toJSONString(wechatData);
//        }
//        System.out.println(datas.size());
//        String type = "2017-07-16"+"2017-08-20";
//        long t3 = System.currentTimeMillis();
//        ESTools.batchByBulk(client,datas,type);
//        long t4 = System.currentTimeMillis();
//        System.out.println("查询数据耗时："+(t2-t));
//        System.out.println("上传数据耗时："+(t4-t3));

    }

    public static boolean indexExists(String index){
        IndicesExistsRequest request = new IndicesExistsRequest(index);
        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        if (response.isExists()) {
            return true;
        }
        return false;
    }

    public static Terms sumValueForDateAndUrl(String index,String type,Map<String,String> aggs,String groupName,String groupField,String termField,String rangeField,String StartDate,String EndDate,String url,String sortAggs,Boolean isAsc){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
//		AggregationBuilder aggre = AggregationBuilders.terms(groupName);
//		if(sortAggs==null){
        AggregationBuilder aggre = AggregationBuilders.terms(groupName).field(groupField);
//		}else{
//			aggre = AggregationBuilders.terms(groupName).field(groupField).order(Terms.Order.aggregation(sortAggs,isAsc));
//		}
        for(String key : aggs.keySet()){
            aggre.subAggregation(AggregationBuilders.sum(key).field(aggs.get(key)));
        }
        BoolQueryBuilder bqb = boolQuery();
        bqb.must(termQuery(termField,url));
        RangeQueryBuilder rqb = rangeQuery(rangeField);
        rqb.from(StartDate).to(EndDate);
        bqb.must(rqb);
//        ps.setQuery(qb);
//        ps.setPostFilter(rqb);
        ps.setQuery(bqb);
        ps.addAggregation(aggre);
        response = ps.get();
        Terms uploadDate = response.getAggregations().get("groupUploadDate");
        return uploadDate;
    }
    /**
     * 根据传入参数获取字段的最大值
     * @param client
     * @param index
     * @param type
     * @param field
     * @return
     */
    public static String maxValue(TransportClient client,String index,String type,String field,String imgurl){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        QueryBuilder qb = matchQuery("imgurl.keyword",imgurl);
        ps.setQuery(qb);
        MaxAggregationBuilder maxAggre = AggregationBuilders.max("agg").field(field);
        response = ps.addAggregation(maxAggre).get();
        Max agg = response.getAggregations().get("agg");
        String max = agg.getValueAsString();
        return max;
    }


//    public static Terms infoForGroupImgUrlAndDate(String index,String type,String rangeField,String StartDate,String EndDate,Map<String,String> aggs,String groupName,String groupField,Integer size){
    public static Terms infoForGroupImgUrlAndDate(String index,String type,String rangeField,String StartDate,String EndDate,Map<String,String> aggs,String groupName,String groupField,Integer size){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        RangeQueryBuilder rqb = rangeQuery(rangeField);
        rqb.from(StartDate).to(EndDate);
        ps.setQuery(rqb);
//        AggregationBuilder Aggre = AggregationBuilders.terms(groupName).field(groupField).size(size).order(Terms.Order.aggregation("acvStats",false))
        AggregationBuilder Aggre = AggregationBuilders.terms(groupName).field(groupField).size(size).order(Terms.Order.aggregation("exposureStats",false));
//                .subAggregation(AggregationBuilders.sum("acvStats").field("acv"));
        for(String key : aggs.keySet()){
//            Aggre.subAggregation(AggregationBuilders.stats(key).field(aggs.get(key)));
            Aggre.subAggregation(AggregationBuilders.sum(key).field(aggs.get(key)));
        }
        response = ps.addAggregation(Aggre).get();
        Terms imgurl = response.getAggregations().get(groupName);
        return imgurl;
//        return imgurl;
//        for (Terms.Bucket entry : imgurl.getBuckets()) {
//            System.out.println(entry.toString());
//            Object o = entry;
//            entry.getKey();      // Term
//            System.out.println(entry.getKeyAsString());
//            entry.getDocCount(); // Doc count
//            System.out.println(entry.getDocCount());
//            Stats acv_stats = entry.getAggregations().get("acvStats");
//            System.out.println("max : "+acv_stats.getMax());
//            System.out.println("avg : "+acv_stats.getAvg());
//        }
    }

    public static void query(String index,String type,String field,String value){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        QueryBuilder qb = matchQuery(field,value);
        response = ps.setQuery(qb).get();
        long count = response.getHits().getTotalHits();
        SearchHit[] searchHits = response.getHits().getHits();
        for(int i = 0; i < searchHits.length; i++) {
            System.out.println(searchHits[i].getSourceAsString());
        }


    }

    public static Map<String,Object> infoForDate(String index,String type,String rangeField,String StartDate,String EndDate,String sort,SortOrder order,Integer from,Integer size){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        RangeQueryBuilder rqb = rangeQuery(rangeField);
        rqb.from(StartDate).to(StartDate);
        ps.setQuery(rqb);
        ps.addSort(sort,order);
        ps.setFrom(from);
        ps.setSize(size);
        response = ps.get();
        long count = response.getHits().getTotalHits();
        SearchHit[] searchHits = response.getHits().getHits();
        Map<String,Object> map = new HashMap();
        map.put("count",count);
        map.put("searchHits",searchHits);
        return map;
    }

    public static Map<String,Aggregation> statsValueForDateAndUrl(String index,String type,Map<String,String> aggs,String rangeField,String StartDate,String EndDate,String url){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        for(String key : aggs.keySet()){
            StatsAggregationBuilder statsAggre = AggregationBuilders.stats(key).field(aggs.get(key));
            ps.addAggregation(statsAggre);
        }
//        QueryBuilder qb = matchQuery("imgurl.keyword",url);
        RangeQueryBuilder rqb = rangeQuery(rangeField);
        rqb.from(StartDate).to(EndDate);
//        ps.setQuery(qb);
//        ps.setPostFilter(rqb);
        BoolQueryBuilder bqb = boolQuery();
        bqb.must(termQuery("imgurl.keyword",url));
        bqb.must(rqb);
        ps.setQuery(bqb);
        response = ps.get();
        Map<String,Aggregation> agg = response.getAggregations().getAsMap();
        return agg;
    }

    public static Map<String,Aggregation> statsValue(String index,String type,Map<String,String> aggs){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        for(String key : aggs.keySet()){
            StatsAggregationBuilder statsAggre = AggregationBuilders.stats(key).field(aggs.get(key));
            ps.addAggregation(statsAggre);
        }
        response = ps.get();
        Map<String,Aggregation> agg = response.getAggregations().getAsMap();
        return agg;
    }

    public static Map<String,Aggregation> statsValueForDate(String index,String type,Map<String,String> aggs,String termField,String rangeField,String startDate,String endDate,String url){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        MatchQueryBuilder mqb = matchQuery(termField,url);
        RangeQueryBuilder rqb = rangeQuery(rangeField);
        rqb.from(startDate).to(endDate);
        BoolQueryBuilder bqb = boolQuery();
        bqb.must(mqb);
        bqb.must(rqb);
        ps.setQuery(bqb);
        for(String key : aggs.keySet()){
            StatsAggregationBuilder statsAggre = AggregationBuilders.stats(key).field(aggs.get(key));
            ps.addAggregation(statsAggre);
        }
        response = ps.get();
        Map<String,Aggregation> agg = response.getAggregations().getAsMap();
        return agg;
    }

//    public static Map<String,Aggregation> statsValueForDate(String index,String type,Map<String,String> aggs,String StartDate,String EndDate){
//        SearchResponse response;
//        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
//        for(String key : aggs.keySet()){
//            StatsAggregationBuilder statsAggre = AggregationBuilders.stats(key).field(aggs.get(key));
//            ps.addAggregation(statsAggre);
//        }
//        RangeQueryBuilder rqb = rangeQuery("uploaddate");
//        rqb.from(StartDate).to(EndDate);
//        ps.setQuery(rqb);
//        response = ps.get();
//        Map<String,Aggregation> agg = response.getAggregations().getAsMap();
//        return agg;
//    }

    public static void test1(){
        BoolQueryBuilder qb = boolQuery();
        qb.must(termQuery("imgurl","http://img.soogif.com/dX9dOZY409d4vft0R7I7nanmIPU4tp7z.jpeg"));
        RangeQueryBuilder rqb = rangeQuery("uploaddate");
        rqb.from("2017-08-08").to("2017-08-20");
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch("wechatupload").setTypes("data");
        ps.setPostFilter(rqb);
        response = ps
                .addSort("exposure", SortOrder.DESC)
                .setQuery(qb)
                .setFrom(0)
                .setSize(20)
                .get();
        long count = response.getHits().getTotalHits();
        SearchHit[] searchHits = response.getHits().getHits();
        for(int i = 0; i < searchHits.length; i++) {
            JSONObject hits = JSONObject.parseObject(searchHits[i].getSourceAsString());
            System.out.println(searchHits[i].getSourceAsString());
        }
    }

    public static Map<String, Aggregation> avgValue(TransportClient client, String index, String type, String termField, String rangeField, String startDate, String endDate, Map<String,String> aggs, String imgurl){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        MatchQueryBuilder mqb = matchQuery(termField,imgurl);
        RangeQueryBuilder rqb = rangeQuery(rangeField);
        rqb.from(startDate).to(endDate);
        BoolQueryBuilder bqb = boolQuery();
        bqb.must(mqb);
        bqb.must(rqb);
        ps.setQuery(bqb);
        for(String key : aggs.keySet()){
            AvgAggregationBuilder avgAggre = AggregationBuilders.avg(key).field(aggs.get(key));
            ps.addAggregation(avgAggre);
        }
        response = ps.get();
        Map<String,Aggregation> agg = response.getAggregations().getAsMap();
        return agg;
    }

    public static Terms sumValueForDateAndUrl(TransportClient client,String index,String type,Map<String,String> aggs,String groupName,String groupField,String termField,String rangeField,String StartDate,String EndDate,String url,String sortAggs,Boolean isAsc){
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        AggregationBuilder aggre = AggregationBuilders.terms(groupName);
        if(sortAggs==null){
            aggre = AggregationBuilders.terms(groupName).field(groupField).size(100);
        }else{
            aggre = AggregationBuilders.terms(groupName).field(groupField).size(100).order(Terms.Order.aggregation("exposureStats",isAsc));
        }
        for(String key : aggs.keySet()){
            aggre.subAggregation(AggregationBuilders.sum(key).field(aggs.get(key)));
        }
        BoolQueryBuilder bqb = boolQuery();
        bqb.must(termQuery(termField,url));
        RangeQueryBuilder rqb = rangeQuery(rangeField);
//		rqb.from(StartDate).to(EndDate);
        if(StartDate==null&&EndDate!=null){
            rqb.to(EndDate);
        }else if(EndDate==null&&StartDate!=null){
            rqb.from(StartDate);
        }else if(StartDate!=null&&EndDate!=null){
            rqb.from(StartDate).to(EndDate);
        }
        bqb.must(rqb);
//        ps.setQuery(qb);
//        ps.setPostFilter(rqb);
        ps.setQuery(bqb);
        ps.addAggregation(aggre);
        response = ps.get();
        Terms uploadDate = response.getAggregations().get(groupName);
        return uploadDate;
    }
}
