package web_data;


import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class checkData {
    public static String DESKey = "BudongGf";

    public static void main(String[] args) {
        Map<Integer,String> collectData = searchCollectData();
//        System.out.println(searchIdForAppl(""));
    }

    public static Map<Integer,String> searchCollectData(){
        Connection conn=null;
        Map<Integer,String> map = new HashMap<Integer, String>();
        try{
            conn= error.DButils.getConnection();
            Statement st=conn.createStatement();
            String sql="SELECT * FROM `BD_MYCOLLECT`;";
            ResultSet rs=st.executeQuery(sql);
            while(rs.next()){
                Integer id  = rs.getInt("id");
                String collectimgurl=rs.getString("collectimgurl").replaceAll("image","img").replaceAll("img1","img");
                if(collectimgurl==null||"".equals(collectimgurl)){
                    System.out.println(id);
                    continue;
                }
                Integer tag = null;
//                String imageId = searchIdForAppl(collectimgurl) == null?searchIdForGAppl(collectimgurl):searchIdForAppl(collectimgurl);
                String imageId = null;
                if(searchIdForAppl(collectimgurl)!=null){
                    imageId = searchIdForAppl(collectimgurl);
                    tag = 0;
                }else{
                    imageId = searchIdForGAppl(collectimgurl);
                    tag = 1;
                }
                if(imageId == null){
                    System.out.println(collectimgurl);
                }
                 searchIdForAppl(collectimgurl);
                String sqlStr = "UPDATE BD_MYCOLLECT SET image_id = '"+imageId+"',tag = "+tag+" WHERE id = "+id+";";
                wirter("d://collect.sql",sqlStr);
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

    public static String searchIdForAppl(String url){
        Connection conn=null;
        String encrypt = null;
        Map<Integer,String> map = new HashMap<Integer, String>();
        try{
            conn= DButils.getConnection();
            Statement st=conn.createStatement();
            String sql="SELECT * FROM G_MATERIAL_APPL WHERE gifurl LIKE '"+url+"%';";
            ResultSet rs=st.executeQuery(sql);
            while(rs.next()){
                Integer id  = rs.getInt("id");
                byte[] bs = Intbyte.int2byte(id);
                encrypt = DESUtil.encrypt(bs, DESKey).replace("=", "").replace("/", "-");
            }
            rs.close();//释放查询结果
            st.close();//释放语句对象
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            DButils.close(conn);
        }
        return encrypt;
    }

    public static String searchIdForGAppl(String url){
        Connection conn=null;
        String encrypt = null;
        Map<Integer,String> map = new HashMap<Integer, String>();
        try{
            conn= DButils.getConnection();
            Statement st=conn.createStatement();
            String sql="SELECT * FROM G_CMS_APPL WHERE gifurl LIKE '"+url+"%';";
            ResultSet rs=st.executeQuery(sql);
            while(rs.next()){
                Integer id  = rs.getInt("id");
                byte[] bs = Intbyte.int2byte(id);
                encrypt = DESUtil.encrypt(bs, DESKey).replace("=", "").replace("/", "-");
            }
            rs.close();//释放查询结果
            st.close();//释放语句对象
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            DButils.close(conn);
        }
        return encrypt;
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
