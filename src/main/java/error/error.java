package error;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class error {
    public static void main(String[] args) {
//        searchErrorData();
        searchNullData();
    }

    public static void errorData(){

    }

    public static Map<String,String> searchNullData(){
        Connection conn=null;
        Map<String,String> map = new HashMap<String, String>();
        try{
            conn=DButils.getConnection();
            Statement st=conn.createStatement();
            String sql="SELECT * FROM `A_CMS_IMAGE_SYNC` WHERE image_url = 'null' ";
            ResultSet rs=st.executeQuery(sql);
            while(rs.next()){
                String serial_code=rs.getString("serial_code");
                String image_url = searchUrlByCode(serial_code);
                String sqlStr = "UPDATE A_CMS_IMAGE_SYNC SET image_url = '"+image_url+"' WHERE serial_code = '"+serial_code+"';";
                System.out.println(sqlStr);
                wirter("d://APInull.sql",sqlStr);
            }
            rs.close();//释放查询结果
            st.close();//释放语句对象
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            DButils.close(conn);
        }
        return map;
    }


    public static Map<String,String> searchErrorData(){
        Connection conn=null;
        Map<String,String> map = new HashMap<String, String>();
        try{
            conn=DButils.getConnection();
            Statement st=conn.createStatement();
            String sql="SELECT * FROM error WHERE image_url = 'http://img.soogif.com/757x67Nw9a064hjpPow0ZKthvchXOIXo.jpg' AND id > 748812 ORDER BY id ASC";
            ResultSet rs=st.executeQuery(sql);
            while(rs.next()){
                String serial_code=rs.getString("serial_code");
                String image_url = searchUrlByCode(serial_code);
                String column_id=rs.getString("column_id");
                String column_alias=rs.getString("column_alias");
                String title=rs.getString("title").replaceAll("\"","");
                String fetch_text=rs.getString("fetch_text").replaceAll("\"","");
                String view_text=rs.getString("view_text").replaceAll("\"","");
                String tag_text=rs.getString("tag_text").replaceAll("\"","");
                String image_remark=rs.getString("image_remark").replaceAll("\"","");
                Integer image_size=rs.getInt("image_size");
                String image_md5=rs.getString("image_md5");
                Integer image_status=rs.getInt("image_status");
                Integer index_status=0;
                String attr_json="";
                Integer sort=rs.getInt("sort");
                Date create_at=rs.getDate("create_at");
                Integer create_by=rs.getInt("create_by");
                String update_at="2017-08-24 13:13:13";
                Integer update_by=24;
                Integer infringement=rs.getInt("infringement");
                String sqlStr = "INSERT INTO `search`.`A_CMS_IMAGE_SYNC` (`column_id`, `column_alias`, `serial_code`, `title`, `fetch_text`, `view_text`,`tag_text`," +
                        "`image_remark`,`image_url`,`image_size`,`image_md5`,`image_status`,`index_status`,`attr_json`,`sort`,`create_at`,`create_by`,`update_at`,`update_by`,`infringement`) " +
                        "VALUES " +
                        "("+column_id+",'"+column_alias+"','"+serial_code+"',\""+title+"\",\""+fetch_text+"\",\""+view_text+"\",\""+tag_text+"\",'"+image_remark+"','"+image_url+"','"+image_size+"','"
                        +image_md5+"','"+image_status+"','"+index_status+"','"+attr_json+"','"+sort+"','"+create_at+"','"+create_by+"','"+update_at+"','"+update_by+"','"+infringement+"'); ";
//                System.out.println(sqlStr);
                wirter("d://APIadd.sql",sqlStr);
            }
            rs.close();//释放查询结果
            st.close();//释放语句对象
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            DButils.close(conn);
        }
        return map;
    }

    public static String searchUrlByCode(String code){
        Connection conn=null;
        Map<String,String> map = new HashMap<String, String>();
        String gifurl = null;
        try{
            conn=DButils.getConnection();
            Statement st=conn.createStatement();
            String sql="SELECT gifurl FROM g_material_appl WHERE CODE = '"+code+"';";
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
}
