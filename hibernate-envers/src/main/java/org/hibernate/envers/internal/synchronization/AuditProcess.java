/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.envers.internal.synchronization;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.action.spi.BeforeTransactionCompletionProcess;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.envers.boot.internal.EnversService;
import org.hibernate.envers.configuration.internal.AuditEntitiesConfiguration;
import org.hibernate.envers.exception.AuditException;
import org.hibernate.envers.internal.entities.mapper.id.IdMapper;
import org.hibernate.envers.internal.revisioninfo.RevisionInfoGenerator;
import org.hibernate.envers.internal.synchronization.work.AuditWorkUnit;
import org.hibernate.envers.internal.synchronization.work.FakeBidirectionalRelationWorkUnit;
import org.hibernate.envers.internal.synchronization.work.ModWorkUnit;
import org.hibernate.envers.internal.synchronization.work.PersistentCollectionChangeWorkUnit;
import org.hibernate.envers.internal.tools.query.QueryBuilder;
import org.hibernate.envers.tools.Pair;
import org.hibernate.envers.veto.spi.AuditVetoer;
import org.hibernate.jpa.QueryHints;
import org.hibernate.resource.jdbc.spi.PhysicalConnectionHandlingMode;
import org.jboss.logging.Logger;

/**
 * @author Adam Warski (adam at warski dot org)
 * @author Chris Cranford
 */
public class AuditProcess implements BeforeTransactionCompletionProcess {
	private static final Logger log = Logger.getLogger( AuditProcess.class );

	private final RevisionInfoGenerator revisionInfoGenerator;
	private final AuditVetoer auditVetoer;
	private final boolean alwaysPersistRevisions;
	private final SessionImplementor session;

	private final LinkedList<AuditWorkUnit> workUnits;
	private final Queue<AuditWorkUnit> undoQueue;
	private final Map<Pair<String, Object>, AuditWorkUnit> usedIds;
	private final Map<Pair<String, Object>, Object[]> entityStateCache;
	private final EntityChangeNotifier entityChangeNotifier;
	private Object revisionData;

	public AuditProcess(RevisionInfoGenerator revisionInfoGenerator, AuditVetoer auditVetoer, boolean alwaysPersistRevisions, SessionImplementor session) {
		this.revisionInfoGenerator = revisionInfoGenerator;
		this.auditVetoer = auditVetoer;
		this.alwaysPersistRevisions = alwaysPersistRevisions;
		this.session = session;

		workUnits = new LinkedList<>();
		undoQueue = new LinkedList<>();
		usedIds = new HashMap<>();
		entityStateCache = new HashMap<>();
		entityChangeNotifier = new EntityChangeNotifier( revisionInfoGenerator, session );
	}

	public void cacheEntityState(Object id, String entityName, Object[] snapshot) {
		final Pair<String, Object> key = new Pair<>( entityName, id );
		if ( entityStateCache.containsKey( key ) ) {
			throw new AuditException( "The entity [" + entityName + "] with id [" + id + "] is already cached." );
		}
		entityStateCache.put( key, snapshot );
	}

	public Object[] getCachedEntityState(Object id, String entityName) {
		final Pair<String, Object> key = new Pair<>( entityName, id );
		final Object[] entityState = entityStateCache.get( key );
		if ( entityState != null ) {
			entityStateCache.remove( key );
		}
		return entityState;
	}

	private void removeWorkUnit(AuditWorkUnit vwu) {
		workUnits.remove( vwu );
		if ( vwu.isPerformed() ) {
			// If this work unit has already been performed, it must be deleted (undone) first.
			undoQueue.offer( vwu );
		}
	}

	public void addWorkUnit(AuditWorkUnit vwu) {
		if ( vwu.containsWork() ) {
			final Object entityId = vwu.getEntityId();

			if ( entityId == null ) {
				// Just adding the work unit - it's not associated with any persistent entity.
				workUnits.offer( vwu );
			}
			else {
				final String entityName = vwu.getEntityName();
				final Pair<String, Object> usedIdsKey = Pair.make( entityName, entityId );

				if ( usedIds.containsKey( usedIdsKey ) ) {
					final AuditWorkUnit other = usedIds.get( usedIdsKey );
					final AuditWorkUnit result = vwu.dispatch( other );

					if ( result != other ) {
						// FIXME: This doesn't work with MergingAuditStrategy
						removeWorkUnit( other );

						if ( result != null ) {
							usedIds.put( usedIdsKey, result );
							workUnits.offer( result );
						}
						// else: a null result means that no work unit should be kept
					}
					// else: the result is the same as the work unit already added. No need to do anything.
				}
				else {
					usedIds.put( usedIdsKey, vwu );
					workUnits.offer( vwu );
				}
			}
		}
	}

//	private void executeInSession(Session session) {
//		// Making sure the revision data is persisted.
//		final Object currentRevisionData = getCurrentRevisionData( session, true );
//
//		AuditWorkUnit vwu;
//
//		// First undoing any performed work units
//		while ( (vwu = undoQueue.poll()) != null ) {
//			vwu.undo( session );
//		}
//
//		while ( (vwu = workUnits.poll()) != null ) {
//			vwu.perform( session, revisionData );
//			entityChangeNotifier.entityChanged( session, currentRevisionData, vwu );
//		}
//	}

	private void executeInSession(Session session) {
		// Making sure the revision data is persisted.
		AuditWorkUnit undoWorkUnit;
		// First undoing any performed work units
		while ( (undoWorkUnit = undoQueue.poll()) != null ) {
			undoWorkUnit.undo( session );
		}

		final List<AuditWorkUnit> missingAddWorkUnits = new ArrayList<>();
		for (Iterator<AuditWorkUnit> iterator = workUnits.iterator(); iterator.hasNext(); ) {
			AuditWorkUnit workUnit = iterator.next();
			if (workUnit.shouldPerform(session, auditVetoer)) {
				if (isWorkUnitWithAddPredecessor(workUnit)) {
					if (auditVetoer.couldHaveMissingCreationAudit(session, workUnit.getEntityName(), workUnit.getEntityId())) {
						if (hasMissingAddAudit(session, workUnit.getEntityName(), workUnit.getEntityId())) {
							missingAddWorkUnits.addAll(extractModWorkUnit(workUnit).generateMissingAddWorkUnits());
						}
					}
				}
			} else {
				iterator.remove();
			}
		}

		if (!missingAddWorkUnits.isEmpty()) {
			final Object missingAddRevisionData = createMissingAddRevisionData(session, alwaysPersistRevisions);
			performAll(session, missingAddWorkUnits, missingAddRevisionData);
		}

		if (!workUnits.isEmpty()) {
			final Object revisionData = getCurrentRevisionData(session, alwaysPersistRevisions);
			performAll(session, workUnits, revisionData);
			workUnits.clear();
		}
	}

	private void performAll(Session session, List<AuditWorkUnit> workUnits, Object revisionData) {
		final Map<Boolean, List<AuditWorkUnit>> workUnitsByIsCollectionChange = workUnits.stream()
				.filter(Objects::nonNull)
				.collect(Collectors.partitioningBy(workUnit -> workUnit instanceof PersistentCollectionChangeWorkUnit));

		workUnitsByIsCollectionChange.get(Boolean.FALSE).forEach(workUnit -> {
			workUnit.perform(session, revisionData);
			entityChangeNotifier.entityChanged(session, revisionData, workUnit);
		});

		workUnitsByIsCollectionChange.get(Boolean.TRUE).forEach(workUnit -> {
			workUnit.perform(session, revisionData);
			entityChangeNotifier.entityChanged(session, revisionData, workUnit);
		});
	}

	private boolean isWorkUnitWithAddPredecessor(AuditWorkUnit auditWorkUnit) {
		return auditWorkUnit instanceof ModWorkUnit || (auditWorkUnit instanceof FakeBidirectionalRelationWorkUnit
				&& ((FakeBidirectionalRelationWorkUnit) auditWorkUnit).getNestedWorkUnit() instanceof ModWorkUnit);
	}

	private ModWorkUnit extractModWorkUnit(AuditWorkUnit auditWorkUnit) {
		if (auditWorkUnit instanceof ModWorkUnit) {
			return (ModWorkUnit) auditWorkUnit;
		} else if (auditWorkUnit instanceof FakeBidirectionalRelationWorkUnit
				&& ((FakeBidirectionalRelationWorkUnit) auditWorkUnit).getNestedWorkUnit() instanceof ModWorkUnit) {
			return (ModWorkUnit) ((FakeBidirectionalRelationWorkUnit) auditWorkUnit).getNestedWorkUnit();
		} else {
			throw new IllegalArgumentException("AuditWorkUnit isn't and also doesn't contain a ModWorkUnit");
		}
	}

	private static final String AUDIT_QUERY_ALIAS = "e";

	private boolean hasMissingAddAudit(Session session, String entityName, Serializable entityId) {
		final EnversService enversService = this.session.getSessionFactory().getServiceRegistry().getService(EnversService.class);
		final IdMapper idMapper = enversService.getEntitiesConfigurations().get(entityName).getIdMapper();
		final String auditEntityName = enversService.getAuditEntitiesConfiguration().getAuditEntityName(entityName);
		try (Session temporarySession = session.sessionWithOptions()
				.connection()
				.autoClose(false)
				.connectionHandlingMode(PhysicalConnectionHandlingMode.DELAYED_ACQUISITION_AND_RELEASE_AFTER_TRANSACTION)
				.openSession()) {
			final QueryBuilder qb = new QueryBuilder(auditEntityName, AUDIT_QUERY_ALIAS, this.session.getSessionFactory());
			idMapper.addIdEqualsToQuery(qb.getRootParameters(), entityId, AUDIT_QUERY_ALIAS, null, true);
			qb.addProjection("count", AUDIT_QUERY_ALIAS, null, true);
			final Number count = (Number) qb.toQuery(temporarySession).setHint(QueryHints.HINT_READONLY, true).getSingleResult();
			return count.intValue() == 0;
		}
	}

	private Object createMissingAddRevisionData(Session session, boolean persist) {
		final Object missingAddRevisionData = revisionInfoGenerator.generate();
		if (persist) {
			revisionInfoGenerator.saveRevisionData( session, missingAddRevisionData );
		}
		return missingAddRevisionData;
	}

	public Object getCurrentRevisionData(Session session, boolean persist) {
		// Generating the revision data if not yet generated
		if ( revisionData == null ) {
			revisionData = revisionInfoGenerator.generate();
		}

		// Saving the revision data, if not yet saved and persist is true
		if ( !session.contains( revisionData ) && persist ) {
			revisionInfoGenerator.saveRevisionData( session, revisionData );
		}

		return revisionData;
	}

	@Override
	public void doBeforeTransactionCompletion(SessionImplementor session) {
		if ( workUnits.size() == 0 && undoQueue.size() == 0 ) {
			return;
		}

		if ( !session.getTransactionCoordinator().isActive() ) {
			log.debug( "Skipping envers transaction hook due to non-active (most likely marked-rollback-only) transaction" );
			return;
		}

		// see: http://www.jboss.com/index.html?module=bb&op=viewtopic&p=4178431
		if ( FlushMode.MANUAL.equals( session.getHibernateFlushMode() ) || session.isClosed() ) {
			Session temporarySession = null;
			try {
				temporarySession = session.sessionWithOptions()
						.connection()
						.autoClose( false )
						.connectionHandlingMode( PhysicalConnectionHandlingMode.DELAYED_ACQUISITION_AND_RELEASE_AFTER_TRANSACTION )
						.openSession();
				executeInSession( temporarySession );
				temporarySession.flush();
			}
			finally {
				if ( temporarySession != null ) {
					temporarySession.close();
				}
			}
		}
		else {
			executeInSession( session );

			// Explicitly flushing the session, as the auto-flush may have already happened.
			session.flush();
		}
	}
}
