package com.taobao.tddl.config.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.taobao.tddl.common.utils.thread.NamedThreadFactory;
import com.taobao.tddl.config.ConfigDataHandler;
import com.taobao.tddl.config.ConfigDataHandlerFactory;
import com.taobao.tddl.config.ConfigDataListener;

public class FileConfigDataHandlerFactory implements ConfigDataHandlerFactory {

    private final static ConcurrentHashMap<String, ConfigDataHandler> filePath         = new ConcurrentHashMap<String, ConfigDataHandler>();

    private final static Executor                                     default_Executor = Executors.newCachedThreadPool(new NamedThreadFactory("configure_file_checker",
                                                                                           true));
    private String                                                    directory        = "";
    private String                                                    appName;
    public static final String                                        configure_prefix = "com.taobao.tddl.";

    public FileConfigDataHandlerFactory(String directory, String appName, Executor executor){
        super();
        this.directory = directory;
        this.appName = appName;
        if (executor != null) {
            this.executor = executor;
        } else {
            this.executor = default_Executor;
        }
    }

    private Executor executor = null;

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String dataId, ConfigDataListener configDataListener) {
        String key = combineDataKey(appName, dataId);
        ConfigDataHandler configDataHandler = filePath.get(key);
        if (configDataHandler == null) {
            synchronized (this) {
                configDataHandler = filePath.get(key);
                // dcl
                if (configDataHandler == null) {
                    configDataHandler = new FileConfigDataHandler(appName,
                        executor,
                        configure_prefix,
                        directory,
                        dataId,
                        configDataListener);
                    ConfigDataHandler tempCdh = filePath.putIfAbsent(key, configDataHandler);
                    if (tempCdh != null) {
                        configDataHandler = tempCdh;
                    }
                }

            }
        }
        configDataHandler.addListener(configDataListener, null);
        return configDataHandler;
    }

    private final String combineDataKey(String appName, String dataId) {
        return appName + "_" + dataId;
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String dataId) {
        return null;
    }

    @Override
    public ConfigDataHandler getConfigDataHandler(String dataId, List<ConfigDataListener> configDataListenerList,
                                                  Executor executor, Map<String, Object> config) {
        return null;
    }
}
