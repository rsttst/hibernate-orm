/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.envers.internal.synchronization.work;

import java.io.Serializable;
import java.util.*;

import org.hibernate.Session;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.envers.RevisionType;
import org.hibernate.envers.internal.entities.RelationDescription;
import org.hibernate.envers.internal.entities.RelationType;
import org.hibernate.envers.internal.entities.mapper.id.IdMapper;
import org.hibernate.envers.internal.synchronization.AuditProcess;
import org.hibernate.envers.internal.tools.EntityTools;
import org.hibernate.envers.veto.spi.AuditVetoer;
import org.hibernate.envers.boot.internal.EnversService;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.HibernateProxy;

/**
 * @author Adam Warski (adam at warski dot org)
 */
public class ModWorkUnit extends AbstractAuditWorkUnit implements AuditWorkUnit {
	private final Map<String, Object> data;
	private final boolean changes;

	private final EntityPersister entityPersister;
	private final Object[] oldState;
	private final Object[] newState;

	public ModWorkUnit(
			SessionImplementor sessionImplementor,
			String entityName,
			EnversService enversService,
			Serializable id,
			EntityPersister entityPersister,
			Object[] newState,
			Object[] oldState) {
		super( sessionImplementor, entityName, enversService, id, RevisionType.MOD );

		this.entityPersister = entityPersister;
		this.oldState = oldState;
		this.newState = newState;
		this.data = new HashMap<>();
		this.changes = enversService.getEntitiesConfigurations().get( getEntityName() ).getPropertyMapper().map(
				sessionImplementor,
				data,
				entityPersister.getPropertyNames(),
				newState,
				oldState
		);
	}

	@Override
	public boolean shouldPerform(Session session, AuditVetoer vetoer) {
		return vetoer.shouldPerformChangeAudit(session, entityName, id, oldState, newState);
	}

	public Map<String, Object> getData() {
		return data;
	}

	@Override
	public boolean containsWork() {
		return changes;
	}

	@Override
	public Map<String, Object> generateData(Object revisionData) {
		fillDataWithId( data, revisionData );

		return data;
	}

	@Override
	public AuditWorkUnit merge(AddWorkUnit second) {
		return this;
	}

	@Override
	public AuditWorkUnit merge(ModWorkUnit second) {
		// In case of multiple subsequent flushes within single transaction, modification flags need to be
		// recalculated against initial and final state of the given entity.
		return new ModWorkUnit(
				second.sessionImplementor,
				second.getEntityName(),
				second.enversService,
				second.id,
				second.entityPersister,
				second.newState,
				this.oldState
		);
	}

	@Override
	public AuditWorkUnit merge(DelWorkUnit second) {
		return second;
	}

	@Override
	public AuditWorkUnit merge(CollectionChangeWorkUnit second) {
		second.mergeCollectionModifiedData( data );
		return this;
	}

	@Override
	public AuditWorkUnit merge(FakeBidirectionalRelationWorkUnit second) {
		return FakeBidirectionalRelationWorkUnit.merge( second, this, second.getNestedWorkUnit() );
	}

	@Override
	public AuditWorkUnit dispatch(WorkUnitMergeVisitor first) {
		return first.merge( this );
	}

	public List<AuditWorkUnit> generateMissingAddWorkUnits() {
		List<AuditWorkUnit> workUnits = new ArrayList<>();
		workUnits.add(new AddWorkUnit(sessionImplementor, entityName, enversService, id, entityPersister, oldState));

		if ( !enversService.getGlobalConfiguration().isGenerateRevisionsForCollections() ) {
			return workUnits;
		}

		// Checks every property of the entity, if it is an "owned" to-one relation to another entity.
		// If the value of that property changed, and the relation is bi-directional, a new revision
		// for the related entity is generated.
		final String[] propertyNames = entityPersister.getPropertyNames();

		for ( int i = 0; i < propertyNames.length; i++ ) {
			final String propertyName = propertyNames[i];
			final RelationDescription relDesc = enversService.getEntitiesConfigurations().getRelationDescription(
					entityName,
					propertyName
			);
			if (relDesc != null && relDesc.isBidirectional() && relDesc.getRelationType() == RelationType.TO_ONE &&
					relDesc.isInsertable()) {
				// Checking for changes
				final Object oldValue = oldState == null ? null : oldState[i];
				if (oldValue != null) {
					workUnits.add(generateCollectionChangeWorkUnit(relDesc, oldValue));
				}
			}
		}

		return workUnits;
	}

	private AuditWorkUnit generateCollectionChangeWorkUnit(RelationDescription relDesc, Object value) {
		// relDesc.getToEntityName() doesn't always return the entity name of the value - in case
		// of subclasses, this will be root class, no the actual class. So it can't be used here.
		String toEntityName;
		Serializable id;

		if ( value instanceof HibernateProxy) {
			final HibernateProxy hibernateProxy = (HibernateProxy) value;
			id = hibernateProxy.getHibernateLazyInitializer().getIdentifier();
			// We've got to initialize the object from the proxy to later read its state.
			value = EntityTools.getTargetFromProxy( sessionImplementor.getSessionFactory(), hibernateProxy );
			// HHH-7249
			// This call must occur after the proxy has been initialized or the returned name will
			// be to the base class which will impact the discriminator value chosen when using an
			// inheritance strategy with discriminators.
			toEntityName = sessionImplementor.bestGuessEntityName( value );
		}
		else {
			toEntityName = sessionImplementor.guessEntityName( value );

			final IdMapper idMapper = enversService.getEntitiesConfigurations().get( toEntityName ).getIdMapper();
			id = (Serializable) idMapper.mapToIdFromEntity( value );
		}

		final Set<String> toPropertyNames = enversService.getEntitiesConfigurations().getToPropertyNames(
				entityName,
				relDesc.getFromPropertyName(),
				toEntityName
		);
		final String toPropertyName = toPropertyNames.iterator().next();

		return new CollectionChangeWorkUnit(sessionImplementor, toEntityName, toPropertyName, enversService,id, value);
	}

}
