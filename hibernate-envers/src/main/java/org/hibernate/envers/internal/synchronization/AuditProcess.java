/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
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
