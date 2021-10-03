/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.envers.veto.internal;

import org.hibernate.Session;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.envers.veto.spi.AuditVetoer;
import org.hibernate.service.ServiceRegistry;

import java.io.Serializable;

public class DefaultAuditVetoer implements AuditVetoer {

	@Override
	public void postInitialize(ServiceRegistry serviceRegistry) {
	}

	@Override
	public boolean shouldPerformCreationAudit(Session session, String entityName, Serializable entityId, Object[] entityState) {
		return true;
	}

	@Override
	public boolean couldHaveMissingCreationAudit(Session session, String entityName, Serializable entityId) {
		return false;
	}

	@Override
	public boolean shouldPerformChangeAudit(
			Session session,
			String entityName,
			Serializable entityId,
			Object[] entityStateOld,
			Object[] entityStateNew) {
		return true;
	}

	@Override
	public boolean shouldPerformCollectionChangeAudit(
			Session session,
			String ownerEntityName,
			Serializable ownerEntityId,
			Object entity,
			String collectionPropertyName) {
		return true;
	}

	@Override
	public boolean shouldPerformPersistentCollectionChangeAudit(
			Session session,
			String ownerEntityName,
			Serializable ownerEntityId,
			String collectionPropertyName,
			PersistentCollection collection) {
		return true;
	}

	@Override
	public boolean shouldPerformDeletionAudit(Session session, String entityName, Serializable entityId, Object[] entityState) {
		return true;
	}

}
