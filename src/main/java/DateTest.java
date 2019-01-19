import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateTest {
    public static void main(String[] args) {
        System.out.println(dateCheck("2017-08-13","2017-08-04"));
    }
    private static boolean dateCheck(String date,String now){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Boolean flag = null;
        try {
            long dateTime = sdf.parse(date).getTime();
            long nowTime = sdf.parse(now).getTime();
            if(dateTime>nowTime){
               flag = false;
            }else{
                flag = true;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return flag;
    }
}
