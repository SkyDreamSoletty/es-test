package FileWX;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by W8616 on 2017/6/9.
 */
public class test1 {

    private static TransportClient client = ESTools.getClient();

    public static void main(String[] args) throws IOException, ParseException {
        readToWrite("D:\\wechat\\20170725_soogif_stat.csv","UTF-8","D:\\20170725_soogif_stat.txt");
//        readTxtFile("D:\\wechat\\20170725_soogif_stat.csv","UTF-8");
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
//        Date date = sdf.parse("20171212");
//        System.out.println(date);
//        System.out.println(sdf2.format(date));
//        Connection conn=null;
//        try{
//            conn=DButils.getConnection();
//            Statement st=conn.createStatement();
//            String s = "82df30b0ee1ee8865e1cd6668b829daa";
//            String sql="select * from G_MATERIAL_APPL where md5 = '"+s+"'";
//            ResultSet rs=st.executeQuery(sql);
//            while(rs.next()){
////                int id=rs.getInt("id");
////                String name=rs.getString("");
//                String gifurl=rs.getString("gifurl");
////                System.out.println(gifurl);
//            }
//            rs.close();//释放查询结果
//            st.close();//释放语句对象
//        }catch(Exception e){
//            e.printStackTrace();
//        }finally {
//            DButils.close(conn);
//        }
//        String name = "D:\\wechat\\20170725_soogif_stat.csv";
//        System.out.println(name.replaceAll("\\D",""));
    }


    /**
     * 按行读取内容后拼接为sql输出
     */
    public static void readToWrite(String path,String encoding,String fileName) throws IOException {
        long t1 = System.currentTimeMillis();
        List<String> strList = readTxtFile(path,encoding);
        long t2 = System.currentTimeMillis();
        long num =t2-t1;
//        System.out.println("获取文件列表："+(t2-t1));
        String sql = null;
        for (int i=0;i<strList.size();i++) {
            String[] tmpArr = strList.get(i).split(",");
            if(!"2".equals(tmpArr[0])||tmpArr.length!=7){
                continue;
            }
            String data_type = tmpArr[0];
            String date = tmpArr[1];
            String md5 = tmpArr[2];
            String exposure = tmpArr[3];//曝光量
            String click_amount = tmpArr[4];//点击量
            String acv = tmpArr[5];//添加收藏量
            String sfv = tmpArr[6];//发送朋友量
            long t3 = System.currentTimeMillis();

//            String img_url = searchUrlByMd5(md5);
            String img_url = searchUrlByMd52(md5);
            long t4 = System.currentTimeMillis();
            long num2 = t4-t3;
//            System.out.println("更具MD5获取gifurl："+(t4-t3));
            String upload_date = path.replaceAll("\\D","");

            try {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
                Date tmpDate = sdf.parse(date);
                date = sdf2.format(tmpDate);
                Date tmpDate2 = sdf.parse(upload_date);
                upload_date = sdf2.format(tmpDate2);
            } catch (ParseException e) {
                e.printStackTrace();
                System.out.println("日期处理出错");
            }

            sql = "INSERT INTO `analysis`.`D_WECHAT_UPLOAD` (`data_type`, `date`, `md5`, `exposure`, `click_amount`, `acv`,`sfv`,`img_url`,`upload_date`) VALUES " +
                    "("+data_type+",'"+date+"','"+md5+"',"+exposure+","+click_amount+","+acv+","+sfv+",'"+img_url+"','"+upload_date+"'); ";

            long t5 = System.currentTimeMillis();
            wirter(fileName, sql);
            long t6 = System.currentTimeMillis();
            long num3 = t6-t5;
            System.out.println("获取文件列表："+num+",更具MD5获取gifurl："+num2+",第"+i+"条写入文件时间："+num3);
        }
    }
    /**
     * 按行读取文件中内容
     * @param filePath
     */
    public static List<String> readTxtFile(String filePath,String encoding){
        List<String> strList = new ArrayList<String>();

        try {

            File file=new File(filePath);
            if(file.isFile() && file.exists()){ //判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file),encoding);//考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                while((lineTxt = bufferedReader.readLine()) != null){
                    strList.add(lineTxt.trim());
//                    System.out.println(lineTxt);
                }
                read.close();
            }else{
                System.out.println("找不到指定的文件");
            }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }
        return strList;
    }
    /**
     * 向文件中追加指定内容
     * @throws IOException
     */
    public static void wirter(String fileName,String line) throws IOException {
        FileOutputStream fos = new FileOutputStream(fileName,true);//文件输出流
        OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");//字符流
        //自动行刷新
        PrintWriter pw = new PrintWriter(osw,true);//具有自动行刷新的缓冲字符输出流
        pw.println(line);//会自动flush
        pw.close();
        osw.close();
        fos.close();
    }

    public static String searchUrlByMd5(String MD5){
        Connection conn=null;
        String gifurl = null;
        try{
            conn=DButils.getConnection();
            Statement st=conn.createStatement();
            String sql="select * from G_MATERIAL_APPL where md5 = '"+MD5+"'";
            ResultSet rs=st.executeQuery(sql);
            while(rs.next()){
                gifurl=rs.getString("gifurl");
            }
            rs.close();//释放查询结果
            st.close();//释放语句对象
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            DButils.close(conn);
        }
        return gifurl;
    }
    public static String searchUrlByMd52(String MD5){

        BoolQueryBuilder qb = boolQuery()
                .must(termQuery("md5","07d62cea0f6b79b21e635f67f66e1ced"));
        SearchResponse response = client.prepareSearch("search_appl")
                .setTypes("appl")
                .setQuery(qb)
                .get();
        SearchHit[] searchHits = response.getHits().getHits();
        return searchHits[0].getSource().toString();
    }
}
