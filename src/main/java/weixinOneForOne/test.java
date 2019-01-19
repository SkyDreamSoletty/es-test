package weixinOneForOne;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;

import java.util.HashMap;
import java.util.Map;

public class test {
    public static void main(String[] args) {
        String str = "IBaX8mKZtGVqdzHPj1KdtlNErECyo9sa\n" +
                "dz14T87iUEXoIacNvB9xZw0YG6hWA8lQ\n" +
                "Do3vAZSAajtYOoimwiU7L70J1lLDkknO\n" +
                "HqKez9Nx9vJR65evq4KvHN97JI3td6H8\n" +
                "MLNkP70csQNi5Bk1Dj9bhBbEG9EkX63p\n" +
                "07kfJrsDDMPFiAcxIn0shIfsIsKybbSb\n" +
                "twCLUPmtJJ2si2EJVwoe5eMmpD1UaR39\n" +
                "X7UguNwEWf6fQ3hG6R33HVKFlK1PLfzB\n" +
                "U8JSqQYshOS6yAtk5FxMK3Oiyd5lye50\n" +
                "wuf761BHZWoFvidehhYrgWKdSOztFVde\n" +
                "g3jzDKJeaZS080nStOKN3KEV87pLDQlF\n" +
                "kkqrFcC3aPgIrR0AKodhtQtogDoFARkV\n" +
                "PBbeDrq4p35BN8e4Aqtpith4VhrH6Lu1\n" +
                "rLCJVN9KxSCDq9h8zRtmtMh4RnLVKBkQ\n" +
                "q2iHW9ZI4d3iHLd5Gmy94L2i0HlsSmjK\n" +
                "Z4aCKT83fNM8oIlrlxQjRRjZS6N3MuQJ\n" +
                "GQsvobaKrtdzqvTvWoCKhhePjveaZtAg\n" +
                "1FU7QTcPyPjxT5ZZSU50mUTxzhZfH1Pc\n" +
                "BjcYmdqHKZXsvMkQsVl2yPe58z9R8Tzj\n" +
                "SBeDnqY6er3A1oQJIQTC4pRXJvwIKKy6\n" +
                "pQt7nfyEFD5EMFBFpTEVxtjEIG765hJh\n" +
                "JkrA9Ku1yhWq0nF3nXQQezCgJusl5x09\n" +
                "7tWOW7jH9QKxPF4NxcOGWDGM6fHciEBs\n" +
                "FWzXHWixd50t3MpG8Pl4sqcnkTkemwr4\n" +
                "vFz91kYWIIXAhjDgSZXrRtuHnCqaT399\n" +
                "gRWORfl8OQsWPle56CWT6o9bQ4JKCHWy\n" +
                "P8bYC0ce9LHJV7CpwFEcSvs6cqFzuahk\n" +
                "QrNUUP4Te29H4TYafD02lUH1j483quf7\n" +
                "R08Se376vhXpJWMDsHdBuZYR8vRoJ1lU\n" +
                "VvpN6yPBeLhaBaQFPWGaQHuAwzUZ8WfP\n" +
                "5KrfnqryVx9c5QLxPeICtiA3eTzIAhv5\n" +
                "iQ6FRL8CwL8fW6lMRIcgW9mDZM4ygVSP\n" +
                "CR9BEk2cbeIlrYhy5pJDMGqueA4Ufeoo\n" +
                "1tHMjKRqA7QsDqAP4YhANE59hDpJxrN1\n" +
                "iqVNhnZKo6PsQYtMeQw88JuulCMrmjar\n" +
                "NGOcWoIQVweqlzPLAPuofJ3eTcMZnbvm\n" +
                "csCxESa8J6IQxE5MTVyGVF1d30czcSNL\n" +
                "UYNbCEJ2fUTYwGWbThjOiCsc4gwNFeeV\n" +
                "JnjIBuOtfm9pWcxlsq0YTWAyhn3vpgi2\n" +
                "UlBZ6vmJn42NB2htdDTuRBYUUUeIW7J1";
//        System.out.println(str.split("\n").length);
        batchQueryForKey(str);
//        System.out.println("dz14T87iUEXoIacNvB9xZw0YG6hWA8lQ\n".replaceAll("\r",""));

    }
    public static void batchQueryForKey(String str){
        JSONArray ja = new JSONArray();
        String[] arr = str.split("\n");
        for(String key : arr){
            ja.add("http://img.soogif.com/"+key+"*");
        }
        Map<String,String> map = new HashMap<String, String>();
        map.put("stats_for_exposure","exposure");
        map.put("ca_for_status","clickamount");
        map.put("acv_for_tats","acv");
        map.put("sfv_for_stats","sfv");
//        Map<String,Aggregation> agg = util.batchQueryForUrl(ESTools.getClient(),"wechatupload","data",ja,"imgurl.keyword","uploaddate","2017-08-02","2017-10-15",map,"group_for_imageUrl","imgurl.keyword","sum_for_exposure","esposure",ja.size());
//        Map<String,Object> ag = agg.get("agg").getMetaData();
        // sr is here your SearchResponse object
//        Range ag = sr.getAggregations().get("agg");
        Range asg = null;
// For each entry

        //        for(String key : agg.get("agg").){
//            Stats ag = (Stats) agg.get(key);
//            sum.put(map.get(key),ag.getSum());
//            System.out.println(map.get(key)+":sum:"+ag.getSum());
//            BigDecimal b = new BigDecimal(ag.getAvg());
//            double tmp = b.setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
//            System.out.println(map.get(key)+":avg:"+tmp);
//            avg.put(aggs.get(key),tmp);
//        }
        Terms imgurl = util.batchQueryForUrl(ESTools.getClient(),"wechatupload","data",ja,"imgurl.keyword","uploaddate","2017-08-02","2017-10-15",map,"group_for_imageUrl","imgurl.keyword","sum_for_exposure","esposure",ja.size());
        int count = 0;
        Map<String,Object> test = new HashMap<String,Object>();
        for(String key:map.keySet()){
            test.put(map.get(key)+":sum:",0);
            test.put(map.get(key)+":avg:",0);
        }
        for (Terms.Bucket entry : imgurl.getBuckets()) {
            System.out.println("-------------");
            count++;
            JSONObject js = new JSONObject();
            for(String key:map.keySet()){
                Stats stats = entry.getAggregations().get(key);
                test.put(map.get(key)+":sum:",Double.valueOf(test.get(map.get(key)+":sum:").toString())+stats.getSum());
                test.put(map.get(key)+":avg:",Double.valueOf(test.get(map.get(key)+":avg:").toString())+stats.getAvg());
//                Sum sum = entry.getAggregations().get("sum_for_exposure");
                js.put(map.get(key),stats.getSum());
                System.out.println("sum:"+stats.getSumAsString());
                System.out.println("avg:"+stats.getAvgAsString());
                System.out.println("max:"+stats.getMaxAsString());
                System.out.println("min:"+stats.getMinAsString());
                System.out.println("count:"+stats.getCountAsString());
//                System.out.println("exposure:"+sum.getValue());
            }
        }
        System.out.println(count);
        System.out.println(JSONObject.toJSONString(test));
    }
}
