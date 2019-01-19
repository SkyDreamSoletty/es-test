package weixinOneForOne;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class fileUtil {
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
                System.out.println("找不到指定文件");
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
}
