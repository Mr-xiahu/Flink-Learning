package cn.xhjava.util;

import cn.xhjava.sftp.IFtpHelper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * @author Xiahu
 * @create 2021/5/8
 */
public class SshExecutorUtil {
    private static final Logger LOG = LoggerFactory.getLogger(SshExecutorUtil.class);

    private String host;
    private int port;
    private String hostusername;
    private String hostpassword;
    private IFtpHelper ftpHelper;

    public SshExecutorUtil(String host, int port, String hostusername, String hostpassword, IFtpHelper ftpHelper) {
        this.host = host;
        this.port = port;
        this.hostusername = hostusername;
        this.hostpassword = hostpassword;
        this.ftpHelper = ftpHelper;
    }

    public void sshExecute(String shellPath, Set<String> fullFileNameToDelete) {
        SshExecuterUtil sshExecuterUtil = new SshExecuterUtil();
        String shell = String.format("/bin/bash %s", shellPath);
        LOG.info("exec ssh command: " + shell);
        SshExecuterUtil.SshConfiguration sshConfiguration =
                new SshExecuterUtil.SshConfiguration(host, port, hostusername, hostpassword);
        try {
            LOG.info("获取ssh连接执行命令" + sshConfiguration);
            Boolean result = sshExecuterUtil.runShell(sshConfiguration, shell);
            LOG.info("执行shell[" + shell + "] 成功, 返回：" + result);
            if (!result) {
                //打印错误日志
                LOG.info("返回失败标识, 获取出错信息");
                sshExecuterUtil.runShell(sshConfiguration,
                        String.format("cat %s", shellPath.replace(".sh", ".log")));
                String msg = String.format("机器:[%s]    用戶：[%s]   cmd:[%s] \n运行失败",
                        new String[]{sshConfiguration.getHost(), sshConfiguration.getUsername(), shell});
                LOG.error(msg);
                throw new RuntimeException();
            } else {
                LOG.info("执行成功, 删除临时文件 " + StringUtils.join(fullFileNameToDelete));
                this.ftpHelper.deleteFiles(fullFileNameToDelete);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }
}
