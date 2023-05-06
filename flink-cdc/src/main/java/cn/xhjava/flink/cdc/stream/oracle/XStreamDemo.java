package cn.xhjava.flink.cdc.stream.oracle;

import oracle.sql.NUMBER;
import oracle.streams.*;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author Xiahu
 * @create 2023/4/23 0023
 * <p>
 * -Djava.library.path=D:\Tool\oracle\instantclient_19_3
 */
public class XStreamDemo {
    private static XStreamOut xsOut;

    private static String xStreamServerName = "dbzxout";


    public static void main(String[] args) throws Exception {

        /**
         * SELECT CURRENT_SCN FROM V$DATABASE;
         * 11   XStreamUtility.POS_VERSION_V1
         * >=12 XStreamUtility.POS_VERSION_V2
         */
        String url = "jdbc:oracle:oci:@192.168.0.67:1521/dbcenter";
        String user = "xstrm";
        String password = "xstrm";
        try {
            //加载数据驱动
            Class.forName("oracle.jdbc.driver.OracleDriver");
            // 连接数据库
            Connection connection = DriverManager.getConnection(url, user, password);
            byte[] startPosition = XStreamUtility.convertSCNToPosition(new NUMBER("35623143", 0), XStreamUtility.POS_VERSION_V1);

            xsOut = XStreamOut.attach((oracle.jdbc.OracleConnection) connection, xStreamServerName,
                    startPosition, 1, 1, XStreamOut.DEFAULT_MODE);

            while (true) {
                //System.out.println("Receiving LCR");
                xsOut.receiveLCRCallback(new LcrEventHandler(), XStreamOut.DEFAULT_MODE);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);

        } finally {
            // 3. disconnect
            if (xsOut != null) {
                try {
                    XStreamOut xsOut2 = xsOut;
                    xsOut = null;
                    xsOut2.detach(XStreamOut.DEFAULT_MODE);
                } catch (StreamsException e) {
                    e.printStackTrace();
                }
            }
        }
    }


}

