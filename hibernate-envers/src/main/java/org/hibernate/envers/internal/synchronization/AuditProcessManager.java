package org.hibernate.envers.internal.synchronization;

import org.hibernate.event.spi.EventSource;

public interface AuditProcessManager {

    AuditProcess get(EventSource session);

}
