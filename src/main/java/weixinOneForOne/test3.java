package weixinOneForOne;

import com.alibaba.fastjson.JSONArray;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.wildcardQuery;

public class test3 {
    public static void main(String[] args) {
        Map<String,String> map = new HashMap<String, String>();
        map.put("stats_for_exposure","exposure");
        map.put("ca_for_status","clickamount");
        map.put("acv_for_tats","acv");
        map.put("sfv_for_stats","sfv");
        ja();
//        batchQueryForUrl(ESTools.getClient(),"wechatupload","data",ja(),"imgurl.keyword",
//                "uploaddate","2017-08-23","2017-10-17",map,
//                "group_uploaddate","uploaddate",null,true,500);
    }
    public static Filter batchQueryForUrl(TransportClient client, String index, String type, JSONArray ja, String boolField, String rangeField, String StartDate, String EndDate, Map<String,String> aggs, String groupName, String groupField, String sort, boolean isAsc, Integer size){
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
        AggregationBuilder aggre = null;
        aggre = AggregationBuilders.filter("range_uploaddate",rqb);
        AggregationBuilder aggre_uploaddate = null;
        if(sort==null){
            aggre_uploaddate = AggregationBuilders.terms(groupName).field(groupField).order(Terms.Order.term(false)).size(size);
        }else{
            aggre_uploaddate = AggregationBuilders.terms(groupName).field(groupField).size(size).order(Terms.Order.aggregation(sort,isAsc));
        }
        AggregationBuilder aggres = null;
        for(String key : aggs.keySet()){
            aggres = AggregationBuilders.stats(key).field(aggs.get(key));
            aggre_uploaddate.subAggregation(aggres);
        }
        aggre.subAggregation(aggre_uploaddate);
        ps.addAggregation(aggre);
        response = ps.get();
        Filter agg = response.getAggregations().get("range_uploaddate");
        return agg;
    }

    public static JSONArray ja(){
        String  str = "3M51ADOj2ME4VGSgXacNR9Dt6y5h3dQN\n" +
                "wStEH3O7jodF3usmDkYq2HcA9RhCYHK4\n" +
                "O8bqsPbOVkQDsWNBLRiJhUMrVJFmFnZ2\n" +
                "LbW4m1F7NafVkjY0CCbMtoE8jGcV2nPH\n" +
                "zVn3ZY2nRUtBt22S9yEmMBJGnoQusWOb\n" +
                "Ygqc53aMy8CGToxM2fb5M0fHfefkl7vV\n" +
                "X7qjoqgTtDa9srJuV5AMf5H7LtyUKoAy\n" +
                "7oGXNbvtuwCJjwvFCae7giaoOP9w6nqA\n" +
                "2E6xfw3j9Bja1u7J5jts8wCkeum83y6W\n" +
                "fpDmqGPvz2bpOyC87u4HxDnq9VV87svm\n" +
                "I5LiwfTxVHdecEaV7YbhPsWleycMfkQp\n" +
                "mL6K4FP81gtYh86a3QtAW6R6sJTCJDLE\n" +
                "zUkTGw8VMTkv1RZVlXiXAKkk9f81UPfY\n" +
                "hOrqBWbMQwBqTxtp6ugzf2UzJAEpD6IE\n" +
                "jOfw0kmXr01uT5BLrrvYcClSP5dZ4tvq\n" +
                "JDFyWd4rTog7lwUtWb0cOwx4d2dkpJo4\n" +
                "TC7UbthZuq1xt0hJqdi5kg9GYmiQsIke\n" +
                "4GlGiH4Vh9Mh2xrcUaR58NLohVRZ0e1d\n" +
                "ERYeViFew4HCPccWkc6FL6KLOEnd1YcY\n" +
                "m103QJjcmC0rFLcUtxZUp8RfB9CUs5Js\n" +
                "2hzvSO8ExykSKu1yBFKRc1qFgKjHzC2c\n" +
                "CZjb2j2iVpJUExy1yiP7Yim8lSKAdHcn\n" +
                "0s7gwqqPTwFPfDXHIWBmzdsorTlnf4CS\n" +
                "IaWPscTzcBZbfcSzgEfiGjhfAVap2LnV\n" +
                "Qfj7YvDBE27fi1Wbf7BSReNSs3vTkryx\n" +
                "BWivQWTJC9rGbR9cUouFEBJbN4Z4Z9dd\n" +
                "jNCDc9I0Fgfn5G4U3ymS85ST9ccjZ5kv\n" +
                "tt8iiGhyRcJWCKGAsRMaowlQLJU11qx5\n" +
                "gB0mtMEHbZoSVO1ibDs0GBZRlvlNAZEY\n" +
                "vtDSDLcdFuPbCHYRCIDhwPc7v65xgDn0\n" +
                "7Wxv3TpQdv4rFORlQBTtpQNYaDOM21rE\n" +
                "btNQUciHnzDA3Nx9usdKecg1thr5Uo5u\n" +
                "XOKMJRkNK4e8YqH9mBI3Fh1m3eXomwIj\n" +
                "LezG1hq2zIULq3JGrTQtX9mgLN1cerq5\n" +
                "aL3EwZMUFrgNUQQRMwiwKh1T89g7mN4n\n" +
                "h1eQIzXHCQhYUidXeNj7DIANcKePc8eZ\n" +
                "xAaphEQmLQTiywgquEHzmaRhip2Q3JW3\n" +
                "oL1WLu7vkqXVhM5qSu5rN1pCNZy9kPbP\n" +
                "VbSqVTyDeNsldpfjqJbhbtbRRPe9ZtB6\n" +
                "g9vbTBM5VWBMdzSieXhFCrSXUhyGzWpd\n" +
                "haKQjZXjrmoVm1viL1aK8rcpsSVNZAUT\n" +
                "J0uOuX6syx2BFRxSfFYZe0Xdx5HxiAS4\n" +
                "cgwa2W1Onfsx5nOUGH6MiT8CDas5l0aH\n" +
                "pYjfPj0HNyub4qqzl0J0v5rD25ASS5kF\n" +
                "cFLob8rHFm4ZaBOSuf0X4Kx3JYKY39iX\n" +
                "SFPX3VlCZ4AkNZJt1BlXNbx3w41HktiG\n" +
                "Jv6fXHlHWeS5MAWHbPRc6A5BiH3N9foS\n" +
                "73wW5pgpXpQcaoGlu1NPJgtRLul2atpt\n" +
                "lubwxaWY0RfiBgTVXsXK5pmrJJvKbgya\n" +
                "FM65J84BvWfl2QxCnQ1BPmdhut6lYu3p\n" +
                "qjZOId0IfuSke4PuyOYTb3AlkruHE4Zr\n" +
                "tNYAcWBvivKE6D89z1PMdgamCGVwIetN\n" +
                "tAQxOdLSh3GjRnDX3nQuq5xaalbAIGfT\n" +
                "Y0GlbWB3Usd7AnYR5v5hZuGdceVeqeCd\n" +
                "H4DUvcHARL6USYnWh0dQO05MYFQw10vN\n" +
                "C7pLl6hnomlYKNPBLeRzEFxu3USwXDMs\n" +
                "cSX9KnSB5b8liisgvP4wFWf2ZGjDSbqp\n" +
                "4fTairUCWfruP2Ga1qkCdmDKyjceg2Ds\n" +
                "MDXj4zlrKx1Y2pbiiKn3EUa5Z548OD8R\n" +
                "pYYGzHMKafhwHleLyKiYSzwZ0K5NsXQG\n" +
                "bJudfGkm52j1Qr7mRvwZwaqtEbKeor5M\n" +
                "Fljepn57zleAc7PbwJnTkf9cQSfB0Eu5\n" +
                "mUCbl4dx1cryxmxfMm4OdFQdksTxMl79\n" +
                "nlZOY0LlKuTOt4ZcWiRKei4S9AhuQtqC\n" +
                "bfWvfVp5gxVUTV0SjzAkkofpv7URdZLh\n" +
                "Dae5RqhNIJiZqnbcxDo3dkS59dDJzKvs\n" +
                "xNYAu34hN4cCjKHd4lTbzU0p4y63OBG4\n" +
                "SEk7koWJmERb4HTRVRTIGzObEjE0uCjC\n" +
                "d0pc8MOEaRgwFxPgTXLBTQpeLGDixFDm\n" +
                "o0RNP6rkQ4JzvY7qpkw28jlHg7MIGryV\n" +
                "NRYG3kbTTNayDiTfHuETJcCLEZsEe7PN\n" +
                "Si7iq6GPrCupOUkxWnSDlTOA3lggN10G\n" +
                "JiGG9GgEBxWKnXPg6H7OqDxbGK54Bngn\n" +
                "U16XwbtYC0WlSuqcf62ZNgf7R6kD7NYg\n" +
                "ypi1DTYjCioDaUzW2kcdMHkLaJdi6Y9r\n" +
                "Gtvl6nI7N9Cmplvrj4aWsqKZ6FSMPV87\n" +
                "dTvHpRgJYBWuh8iP1qKfNxmLtICjahhG\n" +
                "GWwdR5NlEs3m811IwRDnmI5NjFGliJPQ\n" +
                "CLCe6g0D6wBT9bR2A3gm4iEA6ERqGo4d\n" +
                "wg5vVeMZYNqYuBU3pROg1PcsbmTSpV7S\n" +
                "KEhdNDpiicHTB8qxR191LXVUDLHzfpZY\n" +
                "JBy3E9gr4x2IrNiN6l3XHj17BQRl5oJg\n" +
                "S41jhI6YjJXjNf4PvyujazwiHXKwbrzu\n" +
                "Lo2qlipL87xPtvvcqvzPKX6l8AipHHZr\n" +
                "5QYmlFHUCm2otPbnuRiyHD8YjmZzfjfy\n" +
                "dbbDvhtJmPZtylmLazhSEu81HJESQS3T\n" +
                "73bVfXf8Z8qSYUBGwdnFUV6vOZm8ddgd\n" +
                "uJ6tTLWlBgpZrg4tAflMbGvt2gm2g2Gu\n" +
                "K8YJTPw3HRluDemEX7Qlos0S3a1Mc3xO\n" +
                "7NRgBLV6dhpp3TKDX8SNM0NGl87APfok\n" +
                "g4rplnvZDbM9rnX5OHBb3PPfQGcDrxvS\n" +
                "BOkTbwkfAqohyJFrHjWw4GCIVjPNhrXx\n" +
                "F65vrqN6AFIbest3kpxGZFBgtNSVKHii\n" +
                "ZUisE5Yrio5etka1hLawRHejbh8k7ERc\n" +
                "saHviDJQcABNxzBJfnQD4mdtN1PN3Cuj\n" +
                "lmQlHrXJn8kN0hmE2iOEFsBLEJnMPCvW\n" +
                "xXpza8LyBXtt2yQo7C75cpY0nmTgoBpr\n" +
                "dOwk6WLJsLoMLms07OUuuK0Bvg0I0uvs\n" +
                "w6q27cg1fSVDhYHjkhC4g4dSWFrzjG7E\n" +
                "Oln3lV27DXyhEoxWPiUynNAtRWi4zGBB\n" +
                "wL6xkXa0mU2cNxelZF8btVuiH8IOilTu\n" +
                "vT7xZVoMpe7x0g8u12GfntPADuSp0bbb\n" +
                "WuCV2IlsC3aM8DpTk0RjcjYyv5jd5GUk\n" +
                "AbaVT6yaL8Zx0N3BQ930gfJxDQweaU3r\n" +
                "1H2VxVetemy1Mhvz2Elc65RP4XZeX868\n" +
                "MGs6y7OHvvAAAUi9sLwjkUXePTmjH3s0\n" +
                "PoDQ9TnNy6JQ3Lv1G56NnYuwZxE74qaQ\n" +
                "YiT1uohdySaO2XvNbG1eTufCF5TjBCnw\n" +
                "M5BTwem1980T8cziQgdkvtfkS2Qqy5SU\n" +
                "GxVRVJf8Imu2kgNUvqu7Af4DlZIYJpL3\n" +
                "MHfMEiKBSzEZsd0hsjQQGSSoFx8C7XJj\n" +
                "dvzXvJNf0XTJfATM35GkVTlJ5dwHyLv5\n" +
                "cQTxZVoleG8svM2zD3SbFdg1525q7FTK\n" +
                "YBwQMxBf5R6P8Y8i21FFr8GVvqNgJKzh\n" +
                "5DZuGp961sAMWInozVxwo3XDD9U84tc5\n" +
                "jwMGI0z5rW0NBerCrfN9ib83wvxG4ldM\n" +
                "mXPNJPPvRew1Velp0GvcyxIdhiELiG4S\n" +
                "UCvptWW9Xjo3mrZzG8hnsMYCOLpypAKB\n" +
                "9G0JP7VN3OHx3NqQh2SjhCDsheBc4Ejn\n" +
                "tvXbkgXEXK31PfZtpxDGDNVhcsktyoZr\n" +
                "gb2zMYge2KrmJnMnzuS1q2jZGJlQpJon\n" +
                "ZVdzzVKqBo9UQ0jwfn3DIKX41FPVqqXN\n" +
                "XPQSojDnLkYLxc2aRGDQpLq6tIXcnQGW\n" +
                "\n";
        JSONArray ja = new JSONArray();
        String[] arr = str.split("\n");
        for(String key : arr){
            String string = "http://img.soogif.com/"+key.replaceAll("\r","").replaceAll("\n","")+"*";
            ja.add(string);
            System.out.println(string);
        }
        return ja;
    }
}
