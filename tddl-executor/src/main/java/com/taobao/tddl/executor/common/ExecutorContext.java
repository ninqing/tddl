package com.taobao.tddl.executor.common;

import com.taobao.tddl.common.client.util.ThreadLocalMap;
import com.taobao.tddl.executor.repo.RepositoryHolder;
import com.taobao.tddl.executor.spi.ITopologyExecutor;
import com.taobao.tddl.optimizer.sequence.ISequenceManager;

/**
 * @author mengshi.sunmengshi 2013-12-4 下午6:16:32
 * @since 5.0.0
 */
public class ExecutorContext {

    private static final String EXECUTOR_CONTEXT_KEY = "_executor_context_";

    private RepositoryHolder    repositoryHolder     = new RepositoryHolder();

    private TopologyHandler     topologyHandler      = null;
    private ITopologyExecutor   topologyExecutor     = null;
    private ISequenceManager    seqManager           = null;

    public static ExecutorContext getContext() {
        return (ExecutorContext) ThreadLocalMap.get(EXECUTOR_CONTEXT_KEY);
    }

    public static void setContext(ExecutorContext context) {
        ThreadLocalMap.put(EXECUTOR_CONTEXT_KEY, context);
    }

    public RepositoryHolder getRepositoryHolder() {
        return repositoryHolder;
    }

    public void setRepositoryHolder(RepositoryHolder repositoryHolder) {
        this.repositoryHolder = repositoryHolder;
    }

    public ITopologyExecutor getTopologyExecutor() {
        return topologyExecutor;
    }

    public void setTopologyExecutor(ITopologyExecutor topologyExecutor) {
        this.topologyExecutor = topologyExecutor;
    }

    public TopologyHandler getTopologyHandler() {
        return topologyHandler;
    }

    public void setTopologyHandler(TopologyHandler topologyHandler) {
        this.topologyHandler = topologyHandler;
    }

    public ISequenceManager getSeqeunceManager() {
        return this.seqManager;
    }

    public void setSeqManager(ISequenceManager seqManager) {
        this.seqManager = seqManager;
    }

}
