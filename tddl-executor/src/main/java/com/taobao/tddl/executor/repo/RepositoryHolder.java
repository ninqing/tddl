package com.taobao.tddl.executor.repo;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.model.Group;
import com.taobao.tddl.common.utils.extension.ExtensionLoader;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.IRepositoryFactory;

public class RepositoryHolder {

    public static final MessageFormat repoNotFoundError = new MessageFormat("repository is not loaded, name is: {0}");
    private Map<String, IRepository>  repository        = new HashMap<String, IRepository>();

    public boolean containsKey(Object repoName) {
        return repository.containsKey(repoName);
    }

    public boolean containsValue(Object repoName) {
        return repository.containsValue(repoName);
    }

    public IRepository get(Object repoName) {
        return repository.get(repoName);
    }

    public IRepository getOrCreateRepository(Group group, Map<String, String> properties, Map connectionProperties) {
        if (get(group.getType().toString()) != null) {
            return get(group.getType().toString());
        }

        synchronized (this) {
            if (get(group.getType().toString()) == null) {
                IRepositoryFactory factory = getRepoFactory(group.getType().toString());
                IRepository repo = factory.buildRepository(group, properties, connectionProperties);
                if (repo == null) {
                    throw new TddlRuntimeException(repoNotFoundError.format(new String[] { group.getType().toString() }));
                }

                try {
                    repo.init();
                } catch (TddlException e) {
                    throw new TddlRuntimeException(e);
                }
                this.put(group.getType().toString(), repo);
            }
        }

        return this.get(group.getType().toString());
    }

    public IRepository getOrCreateRepository(String group, Map<String, String> properties, Map connectionProperties) {
        if (get(group) != null) {
            return get(group);
        }

        synchronized (this) {
            if (get(group) == null) {
                IRepositoryFactory factory = getRepoFactory(group);
                IRepository repo = factory.buildRepository(null, properties, connectionProperties);
                if (repo == null) {
                    throw new TddlRuntimeException(repoNotFoundError.format(group));
                }

                try {
                    repo.init();
                } catch (TddlException e) {
                    throw new TddlRuntimeException(e);
                }
                this.put(group.toString(), repo);
            }
        }

        return this.get(group.toString());
    }

    private IRepositoryFactory getRepoFactory(String repoName) {
        IRepositoryFactory factory = ExtensionLoader.load(IRepositoryFactory.class, repoName);

        if (factory == null) {
            throw new TddlRuntimeException(repoNotFoundError.format(repoName));
        }
        return factory;
    }

    public IRepository put(String repoName, IRepository repo) {
        return repository.put(repoName, repo);
    }

    public Set<Entry<String, IRepository>> entrySet() {
        return repository.entrySet();
    }

    public Map<String, IRepository> getRepository() {
        return repository;
    }

    public void setRepository(Map<String, IRepository> reponsitory) {
        this.repository = reponsitory;
    }

}
