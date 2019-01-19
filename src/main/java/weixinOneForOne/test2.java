package weixinOneForOne;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;

import java.text.SimpleDateFormat;
import java.util.Date;

public class test2 {
    public static void main(String[] args) {
//        String index = "uploaddate_2017090120170904";
//        Long l1 = countForIndexDocs(index);
//        int l2 = count("2017-09-01","2017-09-04");
        Long a = 1505001600000l;
        Date date = new Date(a);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        System.out.println(sdf.format(date));
//        DeleteIndexResponse response = ESTools.getClient().admin().indices().prepareDelete("uploaddate_2017081720170826")
//                .execute().actionGet();
//
//        System.out.println(response.toString());

//        System.out.println(l1);
//        System.out.println(l2);
//        System.out.println(l2-l1);
//        System.out.println(l2*0.05);
//        getData(index,new Long(l1).intValue());
//        while (!((Math.abs(l2 - l1) < l2 * 0.05))){
//            System.out.println(countForIndexDocs(index));
//        }

    }

    public static Long countForIndexDocs(String index){
        return util.countForIndexDocs(ESTools.getClient(),index,"data");
    }

    public static int count(String startTime, String endTime){
        return new Long(util.count(ESTools.getClient(),"wechatupload","data","imgUrlRange","imgurl.keyword",null,"uploaddate",startTime,endTime)).intValue();
    }

    public static void getData(String index,Integer size){
        long t1 = System.currentTimeMillis();
        util.getDataToCsv(ESTools.getClient(),index,"data",size);
        long t2 = System.currentTimeMillis();
        System.out.println("获取数据耗时："+(t2-t1)+"毫秒");
    }

}
