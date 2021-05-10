package cn.xhjava.flink.stream.sink;

import cn.xhjava.domain.Student5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;

/**
 * @author Xiahu
 * @create 2021/4/26
 */
public class FileSink extends RichSinkFunction<Student5> implements CheckpointedFunction {
    private BufferedWriter bufferedWriter;
    private SimpleDateFormat simpleDateFormat;
    private String sinkFilePath;


    public FileSink(String sinkFilePath) {
        this.sinkFilePath = sinkFilePath;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        bufferedWriter = new BufferedWriter(new FileWriter(new File(sinkFilePath)));
        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void invoke(Student5 value, Context context) throws Exception {
        StringBuffer sb = new StringBuffer();
        long currentTime = value.getCurrentTime();
        String format = simpleDateFormat.format(currentTime);
        sb.append(value.getId())
                .append(",")
                .append(format);
        bufferedWriter.write(sb.toString());
        bufferedWriter.newLine();


    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        bufferedWriter.flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public void close() throws Exception {
        bufferedWriter.close();
    }
}
