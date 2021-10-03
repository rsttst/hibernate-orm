package org.hibernate.envers.internal.synchronization;

import org.hibernate.Session;
import org.hibernate.action.spi.BeforeTransactionCompletionProcess;
import org.hibernate.envers.internal.synchronization.work.AuditWorkUnit;

public interface AuditProcess extends BeforeTransactionCompletionProcess {

    void cacheEntityState(Object id, String entityName, Object[] snapshot);

    Object[] getCachedEntityState(Object id, String entityName);

    void addWorkUnit(AuditWorkUnit vwu);

    Object getCurrentRevisionData(Session session, boolean persist);

}
