package cn.xhjava.flink_12_savepoint;

import cn.xhjava.util.ExceptionUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * @author Xiahu
 * @create 2021/12/24 0024
 */
@Slf4j
public class CheckpointUtil {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static Tuple2<String, String> getLastChecpointInfo(String checkpointBasePath) {
        Tuple2<String, String> result = null;

        FileSystem fs = null;
        try {
            fs = FileSystem.get(new Configuration());
            FileStatus[] fileStatuses = fs.listStatus(new Path(checkpointBasePath));
            long maxAccessTime = 0;
            FileStatus currentCheckpointDir = null;
            for (FileStatus file : fileStatuses) {
                if (file.isDirectory()) {
                    //获取最大创建时间文件
                    long accessTime = file.getModificationTime();
                    if (maxAccessTime < accessTime) {
                        maxAccessTime = accessTime;
                        currentCheckpointDir = file;
                    }

                }
            }
            if (null != currentCheckpointDir) {
                //获取具体checkpoint路径
                FileStatus[] checkpointDirStatus = fs.listStatus(currentCheckpointDir.getPath());
                for (FileStatus file : checkpointDirStatus) {
                    if (file.getPath().toString().contains("chk-")) {
                        result = new Tuple2<String, String>(sdf.format(new Date(file.getModificationTime())), file.getPath().toString());
                    }
                }
            }
        } catch (IOException e) {
            log.info(ExceptionUtil.getStackTrace(e));
        } finally {
            if (null != fs) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        return result;
    }


    public static void main(String[] args) throws IOException {
        Tuple2<String, String> lastChecpointInfo = getLastChecpointInfo("/flink-checkpoints");
        System.out.println(lastChecpointInfo);
    }
}
