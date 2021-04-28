import java.text.SimpleDateFormat;

/**
 * @author Xiahu
 * @create 2021/4/26
 */
public class Test {

    @org.junit.Test
    public void test(){
        long time = System.currentTimeMillis() - (24 * 60 * 60 * 1000);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String format = simpleDateFormat.format(time);
        System.out.println(format);
    }
}
