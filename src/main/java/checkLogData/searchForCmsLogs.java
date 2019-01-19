package checkLogData;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import wechatuploaddata.DButils;
import wechatuploaddata.WechatUploadData;
import weixinOneForOne.ESTools;
import weixinOneForOne.fileUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.index.query.QueryBuilders.*;

public class searchForCmsLogs {

    private static final TransportClient client = ESTools.getClient();

    private static final String index = "cms_logs";

    private static final String type = "cmslogs";
    private static SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
    private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                "match": {
//                "user.keyword": "wb_xutt"
//                "match": {
//                "operation.keyword": "setSubText"
//                "match": {
//                "status.keyword": "true"
//                "range": {
//                "Timestamp.keyword": {
//                    "gte": "09/Jan/2018",
//                            "lte": "10/Jan/2018"

    public static void main(String[] args) throws ParseException, IOException {
            /*
            wb_xutt 1566
            wb_12zwj 1566
            wb_11cq  334
            wb_10xqc 1409
            baishan02 1053
            baishan03 1759
            baishan05 2038
            */
//        searchDataForUserDateAndOperation("wb_xutt","09/Jan/2018","10/Jan/2018","setSubText"); //1566
//        searchDataForUserDateAndOperation("wb_12zwj","09/Jan/2018","10/Jan/2018","setSubText"); //334
        searchDataForUserDateAndOperation("wb_11cq","09/Jan/2018","10/Jan/2018","setSubText"); //1409
//        searchDataForUserDateAndOperation("wb_10xqc","09/Jan/2018","10/Jan/2018","setSubText"); //535
//        searchDataForUserDateAndOperation("wb_baishan02","09/Jan/2018","10/Jan/2018","setSubText"); //1053
//        searchDataForUserDateAndOperation("wb_baishan03","09/Jan/2018","10/Jan/2018","setSubText"); //1759
//        searchDataForUserDateAndOperation("wb_baishan05","09/Jan/2018","10/Jan/2018","setSubText"); //2038

//        searchDataForWB_UserDateAndOperation("wb_*","09/Jan/2018","10/Jan/2018","setSubText");

//        Connection conn=null;
//        conn= DButils.getConnection();
//        searchIdForCode(conn,"G160712100007");
    }

    public static void searchDataForUserDateAndOperation(String user,String bigen,String end,String Operation) throws ParseException {
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        BoolQueryBuilder bqb = boolQuery();
        MatchQueryBuilder mqb1 = matchQuery("user.keyword",user);
        MatchQueryBuilder mqb2 = matchQuery("operation.keyword","setSubText");
        MatchQueryBuilder mqb3 = matchQuery("status.keyword","true");
        RangeQueryBuilder rqb = rangeQuery("Timestamp.keyword");
        rqb.gte(bigen);
        rqb.lte(end);
        bqb.must(mqb1);
        bqb.must(mqb2);
        bqb.must(mqb3);
        bqb.must(rqb);
        ps.setQuery(bqb);
        ps.setSize(10000);
        response = ps.get();
        long count = response.getHits().getTotalHits();
        System.out.println(count);
        SearchHit[] searchHits = response.getHits().getHits();

        Connection conn=null;
        conn= DButils.getConnection();

        for(int i = 0; i < searchHits.length; i++) {
//            System.out.println(searchHits[i].getSourceAsString());
            JSONObject hits = JSONObject.parseObject(searchHits[i].getSourceAsString());
            String userStr = hits.getString("user");
            String pic_number = hits.getString("pic-number");
            Integer appl_id = 0;
            if(!isNumeric(pic_number)){
                appl_id = searchIdForCode(conn,pic_number);
            }else{
                appl_id = Integer.valueOf(pic_number);
            }
            String operation = hits.getString("operation");
            String timeStamp = hits.getString("Timestamp");
            Date tmp = sdf.parse(timeStamp);
            timeStamp = sdf2.format(tmp);
            String status = hits.getString("status");
//            System.out.println(userStr+":"+pic_number+":"+operation+":"+timeStamp);
            String sql = "INSERT INTO `gms_java`.`cms_logs_tmp` (`appl_id`, `pic_number`, `user`, `operation`,`status`,`timestamp`) VALUES ('"+appl_id+"', '"+pic_number+"', '"+userStr+"', '"+operation+"', '"+status+"', '"+timeStamp+"'); ";
            System.out.println(sql);
        }

        DButils.close(conn);

    }

    public static void searchDataForWB_UserDateAndOperation(String user,String bigen,String end,String Operation) throws ParseException, IOException {
        SearchResponse response;
        SearchRequestBuilder ps = client.prepareSearch(index).setTypes(type);
        BoolQueryBuilder bqb = boolQuery();
        WildcardQueryBuilder wqb = wildcardQuery("user.keyword",user);
        MatchQueryBuilder mqb2 = matchQuery("operation.keyword","setSubText");
        MatchQueryBuilder mqb3 = matchQuery("status.keyword","true");
        RangeQueryBuilder rqb = rangeQuery("Timestamp.keyword");
        rqb.gte(bigen);
        rqb.lte(end);
        bqb.must(wqb);
        bqb.must(mqb2);
        bqb.must(mqb3);
        bqb.must(rqb);
        ps.setQuery(bqb);
        ps.setSize(100000);
        response = ps.get();
        long count = response.getHits().getTotalHits();
        System.out.println(count);
        SearchHit[] searchHits = response.getHits().getHits();

        Connection conn=null;
        conn= DButils.getConnection();

        for(int i = 0; i < searchHits.length; i++) {
//            System.out.println(searchHits[i].getSourceAsString());
            JSONObject hits = JSONObject.parseObject(searchHits[i].getSourceAsString());
            String userStr = hits.getString("user");
            String pic_number = hits.getString("pic-number");
            Integer appl_id = 0;
            if(!isNumeric(pic_number)){
                appl_id = searchIdForCode(conn,pic_number);
            }else{
                appl_id = Integer.valueOf(pic_number);
            }
            String operation = hits.getString("operation");
            String timeStamp = hits.getString("Timestamp");
            Date tmp = sdf.parse(timeStamp);
            timeStamp = sdf2.format(tmp);
            String status = hits.getString("status");
//            System.out.println(userStr+":"+pic_number+":"+operation+":"+timeStamp);
            String sql = "INSERT INTO `gms_java`.`cms_logs_tmp2` (`appl_id`, `pic_number`, `user`, `operation`,`status`,`timestamp`) VALUES ('"+appl_id+"', '"+pic_number+"', '"+userStr+"', '"+operation+"', '"+status+"', '"+timeStamp+"'); ";
            fileUtil.wirter("d:/tmp_sql_wb.sql",sql);
//            System.out.println(sql);
        }

        DButils.close(conn);

    }

    public static Integer searchIdForCode(Connection conn,String code){
//        Connection conn=null;
        Map<String,String> map = new HashMap<String, String>();
        Integer id = null;
        try{
//            conn= DButils.getConnection();
            Statement st=conn.createStatement();
            String sql="select id from G_MATERIAL_APPL WHERE code ='"+code+"';";
            ResultSet rs=st.executeQuery(sql);
            while(rs.next()){
                id = rs.getInt("id");
            }
            rs.close();//释放查询结果
            st.close();//释放语句对象
        }catch(Exception e){
            e.printStackTrace();
        }finally {
//            DButils.close(conn);
        }
        return id;
    }

    public static boolean isNumeric(String str){
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher isNum = pattern.matcher(str);
        if( !isNum.matches() ){
            return false;
        }
        return true;
    }


}
