/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.envers.veto.spi;

import org.hibernate.Session;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.service.ServiceRegistry;

import java.io.Serializable;

public interface AuditVetoer {

	void postInitialize(ServiceRegistry serviceRegistry);

	boolean shouldPerformCreationAudit(Session session, String entityName, Serializable entityId, Object[] entityState);

	boolean couldHaveMissingCreationAudit(Session session, String entityName, Serializable entityId);

	boolean shouldPerformChangeAudit(
			Session session,
			String entityName,
			Serializable entityId,
			Object[] entityStateOld,
			Object[] entityStateNew);

	boolean shouldPerformCollectionChangeAudit(
			Session session,
			String ownerEntityName,
			Serializable ownerEntityId,
			Object entity,
			String collectionPropertyName);

	boolean shouldPerformPersistentCollectionChangeAudit(
			Session session,
			String ownerEntityName,
			Serializable ownerEntityId,
			String collectionPropertyName,
			PersistentCollection collection);

	boolean shouldPerformDeletionAudit(Session session, String entityName, Serializable entityId, Object[] entityState);

}
