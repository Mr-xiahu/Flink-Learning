package cn.xhjava.util;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class SshExecuterUtil {

    private static final Logger LOG = LoggerFactory.getLogger(SshExecuterUtil.class);
    private static final int TIMEOUT = 3000;

    /*public Session getSession(String host, int port, String user, String password) throws Exception {
        return getSession(new SshConfiguration(host, port, user, password));
    }*/

    public Session getSession(SshConfiguration sshConfiguration) throws Exception {
        JSch jsch = new JSch();

        Session session = jsch.getSession(sshConfiguration.getUsername(),
                sshConfiguration.getHost(),
                sshConfiguration.getPort());
        // 是否通过密钥连接
        if (sshConfiguration.getPassword() == null) {
            jsch.addIdentity(sshConfiguration.getKey());
        } else {
            session.setPassword(sshConfiguration.getPassword());
        }
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect();
        return session;
    }


    public boolean vaildateConnect(String host, String username) {
        return vaildateConnect(new SshConfiguration(host, username));
    }


    public boolean vaildateConnect(String host, int port, String username, String password) {
        return vaildateConnect(new SshConfiguration(host, port, username, password));
    }

    public boolean vaildateConnect(SshConfiguration sshConfiguration) {
        Session session = null;
        Channel channel = null;
        try {
            session = getSession(sshConfiguration);

            channel = session.openChannel("exec");
            channel.connect();

            return channel.isConnected();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("脚本运行异常：", e);
            return false;
        } finally {
            try {
                if (session != null) {
                    session.disconnect();
                }
                if (channel != null) {
                    channel.disconnect();
                }
            } catch (Exception e) {
                LOG.error("ssh 连接关闭异常：", e);
                return false;
            }

        }
    }


    public boolean runShell(String host, String username, String cmd) {
        return runShell(new SshConfiguration(host, username), cmd);
    }

    public boolean runShell(String host, int port, String user, String password, String cmd) {
        return runShell(new SshConfiguration(host, port, user, password), cmd);
    }

    public String runShellToStr(String host, int port, String user, String password, String cmd) {
        return runShellToStr(new SshConfiguration(host, port, user, password), cmd);
    }

    public String runShellToStr(String host, String username, String cmd) {
        return runShellToStr(new SshConfiguration(host, username), cmd);
    }

    public String runShellToStr(SshConfiguration sshConfiguration, String cmd) {
        Session session = null;
        Channel channel = null;
        BufferedReader reader = null;
        try {
            session = getSession(sshConfiguration);

            long start = System.currentTimeMillis();
            channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand("source /etc/profile && source ~/.bashrc && source ~/.bash_profile &&" + cmd);

            // channel.setInputStream(null);
            // ((ChannelExec) channel).setErrStream(System.err);

            InputStream in = channel.getInputStream();
            reader = new BufferedReader(new InputStreamReader(in));

            channel.connect();
            String buf = null;
            StringBuffer cmdResult = new StringBuffer();
            while ((buf = reader.readLine()) != null) {
                // System.out.println(buf);
                cmdResult.append(buf);
                cmdResult.append("\n");
                LOG.info(buf);
            }
            return cmdResult.toString();

        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("脚本运行异常：", e);
            return null;
        } finally {
            try {
                if (session != null) {
                    session.disconnect();
                }
                if (channel != null) {
                    channel.disconnect();
                }
                if (reader != null) {
                    reader.close();
                }
            } catch (Exception e) {
                LOG.error("ssh 连接关闭异常：", e);
                return null;
            }
        }
    }

    public boolean runShell(SshConfiguration sshConfiguration, String cmd) {
        Session session = null;
        Channel channel = null;
        BufferedReader reader = null;
        try {
            session = getSession(sshConfiguration);

            long start = System.currentTimeMillis();
            channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand("source /etc/profile && source ~/.bashrc && source ~/.bash_profile &&" + cmd);

            // channel.setInputStream(null);
            // ((ChannelExec) channel).setErrStream(System.err);

            InputStream in = channel.getInputStream();
            reader = new BufferedReader(new InputStreamReader(in));

            channel.connect();
            String buf = null;
            String cmdResult = null;
            while ((buf = reader.readLine()) != null) {
                // System.out.println(buf);
                cmdResult = buf;
                LOG.info(buf);
            }


            LOG.info("脚本运行时间{}", String.valueOf(System.currentTimeMillis() - start));
            LOG.info("运行结果：" + cmdResult);
            if (cmdResult != null && Integer.parseInt(cmdResult) == 0) {
                return true;
            } else {
                return false;
            }

        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("脚本运行异常：", e);
            return false;
        } finally {
            try {
                if (session != null) {
                    session.disconnect();
                }
                if (channel != null) {
                    channel.disconnect();
                }
                if (reader != null) {
                    reader.close();
                }
            } catch (Exception e) {
                LOG.error("ssh 连接关闭异常：", e);
                return false;
            }
        }

    }

    public static void main(String[] args) {
        SshExecuterUtil sshExecuterUtil = new SshExecuterUtil();
        // sshExecuterUtil.runShell("192.168.199.202","ls /home/huangjing/");
        //sshExecuterUtil.runShell("192.168.6.218", "huangjing", "java -server -Xms1g -Xmx1g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/home/huangjing/git/DataX/target/datax/datax/log -Xms1g -Xmx1g -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/home/huangjing/git/DataX/target/datax/datax/log -Dloglevel=info -Dfile.encoding=UTF-8 -Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener -Djava.security.egd=file:///dev/urandom -Ddatax.home=/home/huangjing/git/DataX/target/datax/datax -Dlogback.configurationFile=/home/huangjing/git/DataX/target/datax/datax/conf/logback.xml -classpath /home/huangjing/git/DataX/target/datax/datax/lib/*:.  -Dlog.file.name=g_datax_mktEqud_json com.alibaba.datax.core.CloudyEngine -jobName mktEqud2_2018-06-07_10:04:13 -ip 192.168.6.218");
        sshExecuterUtil.runShell("192.168.0.111", "root", "ls;echo $?");
        sshExecuterUtil.runShellToStr("192.168.0.111", "root", "uptime");
        // sshExecuterUtil.runShell("192.168.199.202", 22, "huangjing", "view,2015", "ls /home/huangjing/");
        //Boolean result = sshExecuterUtil.vaildateConnect("192.168.0.111", "root");
        //System.out.println(result);
    }

    public static class SshConfiguration {
        private String key = "~/.ssh/id_rsa";
        private String host;
        private int port = 22;
        private String username;
        private String password;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public SshConfiguration(String host, int port, String username, String password) {
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
        }

        public SshConfiguration(String host, String username) {
            this.host = host;
            this.username = username;
        }

        public SshConfiguration() {
        }
    }
}
