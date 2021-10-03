package org.hibernate.envers.internal.synchronization;

import org.hibernate.Transaction;
import org.hibernate.action.spi.AfterTransactionCompletionProcess;
import org.hibernate.action.spi.BeforeTransactionCompletionProcess;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.envers.internal.revisioninfo.RevisionInfoGenerator;
import org.hibernate.envers.veto.spi.AuditVetoer;
import org.hibernate.event.spi.EventSource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PerTransactionAuditProcessManager implements AuditProcessManager {
    private final Map<Transaction, AuditProcess> auditProcesses;
    private final RevisionInfoGenerator revisionInfoGenerator;
    private final AuditVetoer auditVetoer;
    private final boolean alwaysPersistRevisions;

    public PerTransactionAuditProcessManager(RevisionInfoGenerator revisionInfoGenerator, AuditVetoer auditVetoer, boolean alwaysPersistRevisions) {
        auditProcesses = new ConcurrentHashMap<>();
        this.revisionInfoGenerator = revisionInfoGenerator;
        this.auditVetoer = auditVetoer;
        this.alwaysPersistRevisions = alwaysPersistRevisions;
    }

    public AuditProcess get(EventSource session) {
        final Transaction transaction = session.accessTransaction();

        AuditProcess auditProcess = auditProcesses.get( transaction );
        if ( auditProcess == null ) {
            // No worries about registering a transaction twice - a transaction is single thread
            auditProcess = new TransactionAuditProcess( revisionInfoGenerator, auditVetoer, alwaysPersistRevisions, session );
            auditProcesses.put( transaction, auditProcess );

            session.getActionQueue().registerProcess(
                    new BeforeTransactionCompletionProcess() {
                        public void doBeforeTransactionCompletion(SessionImplementor session) {
                            final AuditProcess process = auditProcesses.get( transaction );
                            if ( process != null ) {
                                process.doBeforeTransactionCompletion( session );
                            }
                        }
                    }
            );

            session.getActionQueue().registerProcess(
                    new AfterTransactionCompletionProcess() {
                        public void doAfterTransactionCompletion(boolean success, SharedSessionContractImplementor session) {
                            auditProcesses.remove( transaction );
                        }
                    }
            );
        }

        return auditProcess;
    }
}
