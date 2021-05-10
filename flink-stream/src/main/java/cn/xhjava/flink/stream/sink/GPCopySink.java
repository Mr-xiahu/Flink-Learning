package cn.xhjava.flink.stream.sink;

import cn.xhjava.domain.OggMsg;
import cn.xhjava.flink.stream.pojo.GreenPlumCopyEvent;
import cn.xhjava.sftp.IFtpHelper;
import cn.xhjava.sftp.SftpHelperImpl;
import cn.xhjava.util.SshExecutorUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Xiahu
 * @create 2021/5/8
 */
@Slf4j
public class GPCopySink extends RichSinkFunction<OggMsg> implements CheckpointedFunction {
    private static final String STATE_NAME = "data-list";
    private LinkedList<String> columnList;
    private Properties prop;
    //用于缓存数据
    private Map<String, GreenPlumCopyEvent> bufferDataMap;
    //状态
    private ListState<Tuple2<String, String>> checkpointState;

    //sftp
    private IFtpHelper ftpHelper;
    private Map<String, BufferedWriter> bufferedWriterMap;

    private static final String DATA_FILE_PATH = "%s/%s_%s.txt";
    private static final String SHELL_FILE_PATH = "%s/%s_%s.shell";
    private static final String LOG_FILE_PATH = "%s/%s_%s.log";
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
    private String rootDir;

    private String shellValue;
    private String gpDatabaseUser;
    private SshExecutorUtil sshExecutor;

    public GPCopySink(Properties prop, LinkedList<String> columnList) {
        this.columnList = columnList;
        this.prop = prop;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ftpHelper = new SftpHelperImpl();
        ftpHelper.loginFtpServer(
                prop.getProperty("greenplum.copy.ftp.host.ip"),
                prop.getProperty("greenplum.copy.ftp.host.user"),
                prop.getProperty("greenplum.copy.ftp.host.password"),
                Integer.valueOf(prop.getProperty("greenplum.copy.ftp.host.port")),
                3000);
        this.sshExecutor = new SshExecutorUtil(
                prop.getProperty("greenplum.copy.ftp.host.ip"),
                Integer.valueOf(prop.getProperty("greenplum.copy.ftp.host.port")),
                prop.getProperty("greenplum.copy.ftp.host.user"),
                prop.getProperty("greenplum.copy.ftp.host.password"),
                this.ftpHelper
        );
        this.shellValue = prop.getProperty("greenplum.copy.shell.value");
        this.gpDatabaseUser = prop.getProperty("greenplum.copy.database.user");
        if (null != bufferDataMap) {
            bufferedWriterMap = new HashMap<>();
            Iterator<Map.Entry<String, GreenPlumCopyEvent>> entryIterator = bufferDataMap.entrySet().iterator();
            while (entryIterator.hasNext()) {
                Map.Entry<String, GreenPlumCopyEvent> entry = entryIterator.next();
                String tableName = entry.getKey();
                GreenPlumCopyEvent event = entry.getValue();
                BufferedWriter writer = fileOperator(event);
                bufferedWriterMap.put(tableName, writer);
            }
        } else {
            bufferDataMap = new HashMap<>();
            bufferedWriterMap = new HashMap<>();
        }
    }

    @Override
    public void invoke(OggMsg oggMsg, Context context) throws Exception {
        GreenPlumCopyEvent event = null;
        String value = oggMsgToString(oggMsg, columnList);
        checkpointState.add(new Tuple2<>(oggMsg.getTable(), value));

        String tableName = oggMsg.getTable();
        //bufferedWriterMap 内是否有对应的bufferWrite,如果有,则用之,如果没有,则创建
        if (bufferedWriterMap.containsKey(tableName)) {
            bufferedWriterMap.get(tableName).write(value);
        } else {
            event = buildGreenPlumCopyEvent(oggMsg.getTable(), value);
            event.setDataList(null);
            BufferedWriter writer = fileOperator(event);
            bufferedWriterMap.put(tableName, writer);
            writer.write(value);
        }
        bufferDataMap.put(oggMsg.getTable(), event);

    }


    /**
     * 程序启动后的初始化,需要从checkpoint处恢复State
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        rootDir = prop.getProperty("greenplum.copy.root.dir");
        log.info("running initializeState");
        checkpointState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Tuple2<String, String>>(STATE_NAME, TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
            @Override
            public TypeInformation<Tuple2<String, String>> getTypeInfo() {
                return super.getTypeInfo();
            }
        })));

        if (context.isRestored()) {
            //判断程序是否重启
            Iterator<Tuple2<String, String>> iterator = checkpointState.get().iterator();
            bufferDataMap = new HashMap<>();
            while (iterator.hasNext()) {
                Tuple2<String, String> tuple = iterator.next();
                if (bufferDataMap.containsKey(tuple.f0)) {
                    GreenPlumCopyEvent event = bufferDataMap.get(tuple.f0);
                    event.getDataList().add(tuple.f1);
                } else {
                    GreenPlumCopyEvent event = buildGreenPlumCopyEvent(tuple.f0, tuple.f1);
                    bufferDataMap.put(tuple.f0, event);
                }
            }
        }
        checkpointState.clear();
        log.info("initializeState over~");

    }

    public GreenPlumCopyEvent buildGreenPlumCopyEvent(String tableName, String line) {
        ArrayList<String> list = new ArrayList<>();
        list.add(line);
        GreenPlumCopyEvent event = new GreenPlumCopyEvent();
        event.setTable(tableName);
        event.setDataList(list);
        String table = tableName.replace(".", "_");
        String time = sdf.format(new Date());
        event.setDataFilePath(String.format(DATA_FILE_PATH, rootDir, table, time));
        event.setShellPath(String.format(SHELL_FILE_PATH, rootDir, table, time));
        event.setLogPath(String.format(LOG_FILE_PATH, rootDir, table, time));
        return event;
    }


    private BufferedWriter fileOperator(GreenPlumCopyEvent event) {
        BufferedWriter writer = null;
        Boolean isExist = this.ftpHelper.dirIsExist(rootDir);
        if (!isExist) {
            this.ftpHelper.mkDirRecursive(rootDir);
        }

        //分别准备三个文件
        try {
            //shell file 写入
            writer = new BufferedWriter(new OutputStreamWriter(this.ftpHelper.getOutputStream(event.getShellPath()), "UTF-8"));
            String[] split = event.getTable().split("\\.");
            String shellValue = String.format(this.shellValue,
                    gpDatabaseUser,
                    split[0],
                    split[1],
                    event.getDataFilePath(),
                    fieldDelimiter,
                    event.getLogPath());
            log.info("exec shell command :" + event.getShellPath() + shellValue);
            writer.write(shellValue);
            writer.flush();

            writer = new BufferedWriter(new OutputStreamWriter(this.ftpHelper.getOutputStream(event.getDataFilePath()), "UTF-8"));
            List<String> dataList = event.getDataList();
            if (null != dataList && dataList.size() > 0) {
                for (String line : dataList) {
                    writer.write(line);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return writer;

    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        log.info("running snapshotState");
        synchronized (this) {
            if (null != bufferedWriterMap) {
                Iterator<Map.Entry<String, BufferedWriter>> entryIterator = bufferedWriterMap.entrySet().iterator();
                while (entryIterator.hasNext()) {
                    Map.Entry<String, BufferedWriter> entry = entryIterator.next();
                    String table = entry.getKey();
                    entry.getValue().flush();
                    entry.getValue().close();
                    GreenPlumCopyEvent event = bufferDataMap.get(table);
                    //执行gp copy 命令,删除源库
                    Set<String> fullFileNameToDelete = new HashSet<>();
                    fullFileNameToDelete.add(event.getDataFilePath());
                    fullFileNameToDelete.add(event.getLogPath());
                    fullFileNameToDelete.add(event.getShellPath());
                    this.sshExecutor.sshExecute(event.getShellPath(), fullFileNameToDelete);
                    bufferedWriterMap.remove(entry.getKey());
                    bufferDataMap.remove(table);
                }
            }
        }
        checkpointState.clear();
        log.info("start delete overdue file");
    }


    @Override
    public void close() throws Exception {
        super.close();
        bufferedWriterMap.clear();
        bufferDataMap.clear();
    }

    private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");
    private static final String fieldDelimiter = "^";
    private static final String SPECIAL_CHARACTER = "[`^]";
    private static final Pattern PATTERN = Pattern.compile(SPECIAL_CHARACTER);

    private static String oggMsgToString(OggMsg oggMsg, LinkedList<String> columnList) {
        if (0 == oggMsg.getAfter().size()) {
            return NEWLINE_FLAG;
        }

        Map<String, String> data = oggMsg.getAfter();

        String column;
        StringBuilder sb = new StringBuilder();
        for (String line : columnList) {
            column = data.get(line);
            if (column == null) {
                column = "\\N";
            } else {
                if (column.indexOf("\u0000") != -1) {
                    column = column.replaceAll("\u0000", "");
                }
                Matcher matcher = PATTERN.matcher(column);
                column = matcher.replaceAll("^$0");
                if (column.indexOf("\n") != -1) {
                    column = column.replaceAll("\n", "\\n");
                }
                if (column.indexOf("\r") != -1) {
                    column = column.replaceAll("\r", "\\r");
                }
                if (column.indexOf("\\") != -1) {
                    column = column.replaceAll("\\\\", "\\\\\\\\");
                }
            }
            sb.append(column).append(fieldDelimiter);
        }
        sb.setLength(sb.length() - 1);
        sb.append(NEWLINE_FLAG);

        return sb.toString();
    }


}
