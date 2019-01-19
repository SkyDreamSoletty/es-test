package dealData.classLabel;

import weixinOneForOne.fileUtil;

import java.io.IOException;
import java.util.List;

public class dealClassLabelData {

//    INSERT INTO `search`.`g_cms_classification` (`title`, `parent`, `level`, `is_leaf`, `icon`, `create_at`, `create_by`, `creater`, `status`) VALUES ('1', '1', '1', '1', '1', '2018-01-10 17:54:49', '1', '1', 't');

    public static void main(String[] args) throws IOException {
//        List<String> titles = fileUtil.readTxtFile("d://明星.txt","UTF-8");
        List<String> titles = fileUtil.readTxtFile("d://体育.txt","UTF-8");
//        List<String> titles = fileUtil.readTxtFile("d://明星.txt","UTF-8");
        for(String title : titles){
            String sql = dealSql(title);
            fileUtil.wirter("d://体育.sql",sql);
        }
    }

    public static String dealSql(String title){
        String sql = "INSERT INTO `search`.`g_cms_classification` " +
                "(`title`, `parent`, `level`, `is_leaf`, `create_at`, `create_by`, `creater`, `status`) VALUES " +
                "('"+title+"', '43', '2', '1', now(), '24', 'test', 't');";
        return sql;
    }
}
