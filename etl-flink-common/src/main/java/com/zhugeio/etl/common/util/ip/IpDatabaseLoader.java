package com.zhugeio.etl.common.util.ip;

import com.zhugeio.etl.common.model.ip.IpDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * IP数据库加载器 - 支持热加载
 *
 * 功能:
 * 1. 从HDFS加载IP数据库文件
 * 2. 定期检查文件更新并热加载
 * 3. 线程安全的读写锁
 */
public class IpDatabaseLoader implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(IpDatabaseLoader.class);
    private static final long serialVersionUID = 1L;

    // 使用 AtomicReference 保证原子性更新
    private transient AtomicReference<IpDatabase> databaseRef;
    private transient ReentrantReadWriteLock lock;
    private transient ScheduledExecutorService reloadExecutor;

    private final String hdfsPath;
    private final boolean enableAutoReload;
    private final long reloadIntervalSeconds;
    private final boolean isHdfsHA;

    private transient volatile long lastModifyTime = 0L;

    public IpDatabaseLoader(String hdfsPath, boolean enableAutoReload, long reloadIntervalSeconds, boolean isHdfsHA) {
        this.hdfsPath = hdfsPath;
        this.enableAutoReload = enableAutoReload;
        this.reloadIntervalSeconds = reloadIntervalSeconds;
        this.isHdfsHA = isHdfsHA;
    }

    /**
     * 初始化加载器
     */
    public void init() throws IOException {
        databaseRef = new AtomicReference<>();
        lock = new ReentrantReadWriteLock();

        // 首次加载
        loadDatabase();

        // 启动定期重载
        if (enableAutoReload) {
            startAutoReload();
        }

        LOG.info("✅ IP数据库加载器初始化成功: {}, 自动重载={}, 间隔={}秒",
                hdfsPath, enableAutoReload, reloadIntervalSeconds);
    }

    /**
     * 加载IP数据库
     */
    private void loadDatabase() throws IOException {
        lock.writeLock().lock();
        try {
            FileStatus fileStatus = getLatestFile(hdfsPath);

            if (fileStatus == null) {
                throw new IOException("IP数据库文件不存在: " + hdfsPath);
            }

            long currentModifyTime = fileStatus.getModificationTime();

            // 如果文件没有更新,跳过加载
            if (currentModifyTime <= lastModifyTime) {
                LOG.debug("IP数据库文件未更新,跳过加载");
                return;
            }

            LOG.info("开始加载IP数据库: {}, 修改时间={}",
                    fileStatus.getPath(),
                    new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(currentModifyTime));

            long startTime = System.currentTimeMillis();

            // 读取文件到内存
            byte[] data = readFileToMemory(fileStatus);

            // 解析IP数据库
            IpDatabase newDatabase = new IpDatabase(data);

            // 原子性更新
            databaseRef.set(newDatabase);
            lastModifyTime = currentModifyTime;

            long elapsed = System.currentTimeMillis() - startTime;
            LOG.info("✅ IP数据库加载成功: 大小={}MB, 耗时={}ms",
                    data.length / 1024 / 1024, elapsed);

        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 查询IP地址
     */
    public String[] query(String ipAddress) {
        lock.readLock().lock();
        try {
            IpDatabase database = databaseRef.get();
            if (database == null) {
                LOG.warn("IP数据库未加载");
                return new String[]{"", "", ""};
            }

            return database.find(ipAddress);

        } catch (Exception e) {
            LOG.error("IP查询失败: {}", ipAddress, e);
            return new String[]{"", "", ""};
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 查询IP地址 (支持IPv6)
     */
    public String[] query(InetAddress inetAddress) {
        lock.readLock().lock();
        try {
            IpDatabase database = databaseRef.get();
            if (database == null) {
                LOG.warn("IP数据库未加载");
                return new String[]{"", "", ""};
            }

            return database.find(inetAddress);

        } catch (Exception e) {
            LOG.error("IP查询失败: {}", inetAddress, e);
            return new String[]{"", "", ""};
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 启动自动重载
     */
    private void startAutoReload() {
        reloadExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "IP-Database-Reloader");
            thread.setDaemon(true);
            return thread;
        });

        reloadExecutor.scheduleAtFixedRate(() -> {
            try {
                loadDatabase();
            } catch (Exception e) {
                LOG.error("定期重载IP数据库失败", e);
            }
        }, reloadIntervalSeconds, reloadIntervalSeconds, TimeUnit.SECONDS);

        LOG.info("✅ IP数据库自动重载已启动: 间隔={}秒", reloadIntervalSeconds);
    }

    /**
     * 获取HDFS最新文件
     */
    private FileStatus getLatestFile(String pathStr) throws IOException {
        Configuration conf = new Configuration();

        if (isHdfsHA) {
            // HA配置 (从你的Config读取)
            // conf.set("fs.defaultFS", ...);
            // conf.set("dfs.nameservices", ...);
        }

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(pathStr);

        if (!fs.exists(path)) {
            return null;
        }

        // 如果是目录,找最新的文件
        if (fs.getFileStatus(path).isDirectory()) {
            FileStatus[] files = fs.listStatus(path);
            FileStatus latest = null;
            long latestTime = 0;

            for (FileStatus file : files) {
                if (!file.isFile()) {
                    continue;
                }
                if (file.getModificationTime() > latestTime) {
                    latestTime = file.getModificationTime();
                    latest = file;
                }
            }

            return latest;
        } else {
            return fs.getFileStatus(path);
        }
    }

    /**
     * 读取HDFS文件到内存
     */
    private byte[] readFileToMemory(FileStatus fileStatus) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        try (InputStream in = fs.open(fileStatus.getPath());
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }

            return out.toByteArray();
        }
    }

    /**
     * 关闭加载器
     */
    public void close() {
        if (reloadExecutor != null) {
            reloadExecutor.shutdown();
            LOG.info("IP数据库自动重载已停止");
        }
    }
}