package wechatuploaddata;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class uploadTest {
    public static void main(String[] args) {
        WechatUploadData wud = new WechatUploadData();
        searchWechatData(wud);
//        ESTools.batchUploadBybulk(null);
    }
    public static WechatUploadData searchWechatData(WechatUploadData wud){
        Connection conn=null;
        Map<String,String> map = new HashMap<String, String>();
        try{
            conn=DButils.getConnection();
            Statement st=conn.createStatement();
            String sql="select * from `D_WECHAT_UPLOAD` limit 1";
            ResultSet rs=st.executeQuery(sql);
            while(rs.next()){
                wud.setDataType(rs.getInt(""));
                String md5=rs.getString("md5");
                String gifurl=rs.getString("gifurl");
                map.put(md5,gifurl);
            }
            rs.close();//释放查询结果
            st.close();//释放语句对象
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            DButils.close(conn);
        }
        return null;
    }
}
