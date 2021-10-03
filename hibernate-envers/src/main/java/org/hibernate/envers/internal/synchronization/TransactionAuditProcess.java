/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.envers.internal.synchronization;

import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.envers.boot.internal.EnversService;
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

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Adam Warski (adam at warski dot org)
 * @author Chris Cranford
 */
public class TransactionAuditProcess implements AuditProcess {
	private static final Logger log = Logger.getLogger( TransactionAuditProcess.class );

	private final RevisionInfoGenerator revisionInfoGenerator;
	private final AuditVetoer auditVetoer;
	private final boolean alwaysPersistRevisions;
	private final SessionImplementor sessionImplementor;

	private final Map<Pair<String, Object>, AuditWorkUnit> workUnitsByNameAndId;
	private final List<AuditWorkUnit> workUnitsWithoutId;

	private final Map<Pair<String, Object>, Object[]> entityStateCache;
	private final EntityChangeNotifier entityChangeNotifier;

	private Object currentRevisionData;

	public TransactionAuditProcess(RevisionInfoGenerator revisionInfoGenerator, AuditVetoer auditVetoer,
								   boolean alwaysPersistRevisions, SessionImplementor sessionImplementor) {
		this.revisionInfoGenerator = revisionInfoGenerator;
		this.auditVetoer = auditVetoer;
		this.alwaysPersistRevisions = alwaysPersistRevisions;
		this.sessionImplementor = sessionImplementor;

		workUnitsByNameAndId = new LinkedHashMap<>();
		workUnitsWithoutId = new ArrayList<>();

		entityStateCache = new HashMap<>();
		entityChangeNotifier = new EntityChangeNotifier( revisionInfoGenerator, sessionImplementor);

		currentRevisionData = null;
	}

	@Override
	public void cacheEntityState(Object id, String entityName, Object[] snapshot) {
		final Pair<String, Object> key = new Pair<>( entityName, id );
		if ( entityStateCache.containsKey( key ) ) {
			throw new AuditException( "The entity [" + entityName + "] with id [" + id + "] is already cached." );
		}
		entityStateCache.put( key, snapshot );
	}

	@Override
	public Object[] getCachedEntityState(Object id, String entityName) {
		final Pair<String, Object> key = new Pair<>( entityName, id );
		final Object[] entityState = entityStateCache.get( key );
		if ( entityState != null ) {
			entityStateCache.remove( key );
		}
		return entityState;
	}

	@Override
	public void addWorkUnit(AuditWorkUnit vwu) {
		if ( !vwu.containsWork() ) {
			return;
		}

		final Object entityId = vwu.getEntityId();
		if ( entityId == null ) {
			workUnitsWithoutId.add(vwu);
		} else {
			final String entityName = vwu.getEntityName();
			workUnitsByNameAndId.compute(
					Pair.make( entityName, entityId ),
					(key, value) -> value != null ? vwu.dispatch(value) : vwu
			);
		}
	}

	@Override
	public Object getCurrentRevisionData(Session session, boolean persist) {
		return currentRevisionData;
	}

	private void executeInSession(Session session) {
		workUnitsWithoutId.addAll(workUnitsByNameAndId.values());
		final List<AuditWorkUnit> allWorkUnits = workUnitsWithoutId;
		final List<AuditWorkUnit> missingAddWorkUnits = new ArrayList<>();
		for (Iterator<AuditWorkUnit> iterator = allWorkUnits.iterator(); iterator.hasNext(); ) {
			AuditWorkUnit workUnit = iterator.next();
			if (!workUnit.shouldPerform(session, auditVetoer)) {
				iterator.remove();
				continue;
			}

			if (isWorkUnitWithAddPredecessor(workUnit)
					&& auditVetoer.couldHaveMissingCreationAudit(session, workUnit.getEntityName(), workUnit.getEntityId())
					&& hasMissingAddAudit(session, workUnit.getEntityName(), workUnit.getEntityId())) {
						missingAddWorkUnits.addAll(extractModWorkUnit(workUnit).generateMissingAddWorkUnits());
			}
		}

		if (!missingAddWorkUnits.isEmpty()) {
			final Object missingAddRevisionData = createRevisionData(session, alwaysPersistRevisions);
			performAll(session, missingAddWorkUnits, missingAddRevisionData);
		}

		if (!allWorkUnits.isEmpty()) {
			final Object revisionData = createRevisionData(session, alwaysPersistRevisions);
			currentRevisionData = revisionData;
			performAll(session, allWorkUnits, revisionData);
			workUnitsWithoutId.clear();
			workUnitsByNameAndId.clear();
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
		final EnversService enversService = this.sessionImplementor.getSessionFactory().getServiceRegistry().getService(EnversService.class);
		final IdMapper idMapper = enversService.getEntitiesConfigurations().get(entityName).getIdMapper();
		final String auditEntityName = enversService.getAuditEntitiesConfiguration().getAuditEntityName(entityName);
		try (Session temporarySession = session.sessionWithOptions()
				.connection()
				.autoClose(false)
				.connectionHandlingMode(PhysicalConnectionHandlingMode.DELAYED_ACQUISITION_AND_RELEASE_AFTER_TRANSACTION)
				.openSession()) {
			final QueryBuilder qb = new QueryBuilder(auditEntityName, AUDIT_QUERY_ALIAS, this.sessionImplementor.getSessionFactory());
			idMapper.addIdEqualsToQuery(qb.getRootParameters(), entityId, AUDIT_QUERY_ALIAS, null, true);
			qb.addProjection("count", AUDIT_QUERY_ALIAS, null, true);
			final Number count = (Number) qb.toQuery(temporarySession).setHint(QueryHints.HINT_READONLY, true).getSingleResult();
			return count.intValue() == 0;
		}
	}

	private Object createRevisionData(Session session, boolean persist) {
		final Object missingAddRevisionData = revisionInfoGenerator.generate();
		if (persist) {
			revisionInfoGenerator.saveRevisionData( session, missingAddRevisionData );
		}
		return missingAddRevisionData;
	}

	@Override
	public void doBeforeTransactionCompletion(SessionImplementor session) {
		if ( workUnitsWithoutId.isEmpty() && workUnitsByNameAndId.isEmpty() ) {
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
