/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.envers.strategy.internal;

import org.hibernate.FlushMode;
import org.hibernate.Hibernate;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.envers.AuditReader;
import org.hibernate.envers.AuditReaderFactory;
import org.hibernate.envers.RevisionType;
import org.hibernate.envers.boot.internal.EnversService;
import org.hibernate.envers.configuration.EnversSettings;
import org.hibernate.envers.configuration.internal.AuditEntitiesConfiguration;
import org.hibernate.envers.configuration.internal.GlobalConfiguration;
import org.hibernate.envers.internal.entities.EntitiesConfigurations;
import org.hibernate.envers.internal.entities.EntityConfiguration;
import org.hibernate.envers.internal.entities.PropertyData;
import org.hibernate.envers.internal.entities.RelationDescription;
import org.hibernate.envers.internal.entities.mapper.ExtendedPropertyMapper;
import org.hibernate.envers.internal.entities.mapper.PersistentCollectionChangeData;
import org.hibernate.envers.internal.entities.mapper.PropertyMapper;
import org.hibernate.envers.internal.entities.mapper.id.IdMapper;
import org.hibernate.envers.internal.entities.mapper.relation.AbstractCollectionMapper;
import org.hibernate.envers.internal.tools.ReflectionTools;
import org.hibernate.envers.query.AuditEntity;
import org.hibernate.envers.strategy.spi.AuditStrategy;
import org.hibernate.envers.tools.Pair;
import org.hibernate.internal.util.collections.ConcurrentReferenceHashMap;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.property.access.spi.Getter;
import org.hibernate.property.access.spi.Setter;
import org.hibernate.resource.jdbc.spi.PhysicalConnectionHandlingMode;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tuple.entity.EntityTuplizer;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;

public class MergingAuditStrategy extends DefaultAuditStrategy implements AuditStrategy {

	// both weak keys, strong values
	// inner map: Pair(String entityName, Object entityId) => Pair(MergeKind mergeKind, Object targetRevision)
	private final ConcurrentReferenceHashMap<Transaction, Map<Pair<String, Object>, Pair<MergeKind, Object>>> mergeInfoByEntityByTransaction;
	private EntitiesConfigurations entitiesConfigurations;
	private AuditEntitiesConfiguration auditEntitiesConfiguration;
	private Getter revisionInfoTimestampGetter;
	private Setter revisionInfoTimestampSetter;
	private boolean revisionInfoTimestampIsDate;

	public MergingAuditStrategy() {
		super();
		this.mergeInfoByEntityByTransaction = new ConcurrentReferenceHashMap<>( // the following parameters are the default, but let's be explicit
				16, ConcurrentReferenceHashMap.ReferenceType.WEAK, ConcurrentReferenceHashMap.ReferenceType.STRONG
		);
	}

	private static Session createTemporarySession(Session baseSession) {
		return baseSession.sessionWithOptions()
				.connection()
				.autoClose(false)
				.connectionHandlingMode(PhysicalConnectionHandlingMode.DELAYED_ACQUISITION_AND_RELEASE_AFTER_TRANSACTION)
				.flushMode(FlushMode.MANUAL)
				.openSession();
	}

	private static <T> T tryCastNonNull(Object object, Class<T> clazz, String description) {
		final T result = tryCast(object, clazz, description);
		if (result == null) {
			throw new RuntimeException("Expected '" + description + "' to be non-null.");
		}
		return result;
	}

	private static <T> T tryCast(Object object, Class<T> clazz, String description) {
		if (!clazz.isInstance(object)) {
			throw new RuntimeException(String.format(Locale.ROOT, "Expected '%s' to be of type '%s' but was '%s'.",
					description, clazz.getName(), object.getClass().getName()));
		}
		return clazz.cast(object);
	}

	@Override
	public void postInitialize(Class<?> revisionInfoClass, PropertyData timestampData, ServiceRegistry serviceRegistry) {
		final EnversService enversService = serviceRegistry.getService(EnversService.class);
		final GlobalConfiguration globalConfiguration = enversService.getGlobalConfiguration();
		if (!(globalConfiguration.isEnableUpdatableRevisions()
				&& globalConfiguration.isGlobalWithModifiedFlag()
				&& globalConfiguration.isRevisionPerTransaction())) {
			throw new RuntimeException(String.format(Locale.ROOT,
					"To use MergingAuditStrategy you have to enable global modified flags (%s), "
							+ "opt into updatable revisions (%s) and "
							+ "enable revision per transaction mode (%s).",
					EnversSettings.GLOBAL_WITH_MODIFIED_FLAG,
					EnversSettings.ENABLE_UPDATABLE_REVISIONS,
					EnversSettings.REVISION_PER_TRANSACTION
			));
		}
		entitiesConfigurations = enversService.getEntitiesConfigurations();
		auditEntitiesConfiguration = enversService.getAuditEntitiesConfiguration();

		revisionInfoTimestampGetter = ReflectionTools.getGetter(
				revisionInfoClass,
				timestampData,
				serviceRegistry
		);
		revisionInfoTimestampSetter = ReflectionTools.getSetter(
				revisionInfoClass,
				timestampData,
				serviceRegistry
		);
		final Class<?> revisionInfoTimestampType = ReflectionTools.getType(revisionInfoClass, timestampData, serviceRegistry);
		revisionInfoTimestampIsDate = revisionInfoTimestampType == java.util.Date.class || revisionInfoTimestampType == java.sql.Date.class;
	}

	@Override
	public void perform(
			Session session,
			String entityName,
			AuditEntitiesConfiguration auditEntitiesConfiguration,
			Serializable entityId,
			Object currentAuditData,
			Object currentRevision
	) {
		final Pair<String, Object> entityNameWithId = Pair.make(entityName, entityId);
		final EntityConfiguration entityConfiguration = entitiesConfigurations.get(entityName);

		final Transaction transaction = session.getTransaction();
		if (!entityConfiguration.isMergeable()
				|| getRevisionTypeFromAuditData(currentAuditData) != RevisionType.MOD) { // only MOD revisions are mergeable
			defaultPerform(session, entityName, entityId, currentAuditData, currentRevision, transaction, entityNameWithId);
			return;
		}

		try (Session temporarySession = createTemporarySession(session)) {
			final AuditReader auditReader = AuditReaderFactory.get(temporarySession);
			final Class<?> entityClass = getEntityClass(entityName);

			final Pair<Object, Object> previousRevisionInfo = findPreviousRevision(auditReader, entityClass, entityName, entityId);
			if (previousRevisionInfo.getSecond() == null) { // no state before the previous revision => default perform
				defaultPerform(session, entityName, entityId, currentAuditData, currentRevision, transaction, entityNameWithId);
				return;
			}

			// un-proxy the revision since we are indirectly accessing fields via reflection
			final Object previousRevision = Hibernate.unproxy(previousRevisionInfo.getFirst());
			final Object beforePreviousRevisionEntity = previousRevisionInfo.getSecond();

			if (!isRevisionInMergeableTimeframe(currentRevision, previousRevision, entityConfiguration)) {
				defaultPerform(session, entityName, entityId, currentAuditData, currentRevision, transaction, entityNameWithId);
				return;
			}

			// grab the current state from the non-temporary session if it already exists there
			final Object currentEntity;
			if (session.contains(entityName, entityId)) {
				currentEntity = session.get(entityName, entityId);
			}
			else {
				currentEntity = temporarySession.get(entityName, entityId);
			}

			/* ==================== REVISION MERGE START ==================== */
			final AbstractEntityPersister entityPersister = getPersisterFromEntityName(session.unwrap(SessionImplementor.class), entityName);
			final String auditEntityName = auditEntitiesConfiguration.getAuditEntityName(entityName);

			final Map<String, Object> mergeAuditData = createAuditEntityData(
					temporarySession.unwrap(SessionImplementor.class),
					entityName,
					entityPersister.getEntityTuplizer(),
					beforePreviousRevisionEntity,
					currentEntity,
					entityId
			);

			final Pair<MergeKind, Object> mergeInfo;
			if (mergeAuditData == null) { // null updatedAuditData means the entity was reverted to its pre-revision state
				final HashMap<String, Object> previousAuditDataId = new HashMap<>();
				previousAuditDataId.put(auditEntitiesConfiguration.getRevisionFieldName(), previousRevision);
				entitiesConfigurations.get(entityName).getIdMapper().mapToMapFromId(temporarySession, previousAuditDataId, entityId);

				temporarySession.clear();
				temporarySession.remove(temporarySession.load(auditEntityName, previousAuditDataId));
				temporarySession.flush();

				mergeInfo = Pair.make(MergeKind.MERGE_BY_UNDO, previousRevision);
			}
			else if (isForwardMergeNecessary(entityConfiguration, mergeAuditData)) { // should we just use 'data' instead of 'updatedAuditData' ?
				attachIdToAuditData(mergeAuditData, temporarySession, entityConfiguration.getIdMapper(), entityId, currentRevision);

				final HashMap<String, Object> previousAuditDataId = new HashMap<>();
				previousAuditDataId.put(auditEntitiesConfiguration.getRevisionFieldName(), previousRevision);
				entitiesConfigurations.get(entityName).getIdMapper().mapToMapFromId(temporarySession, previousAuditDataId, entityId);

				temporarySession.clear();
				if (alwaysPersistRevisions) {
					session.flush(); // flush the parent session so that we can see the revision info entity
				} else {
					temporarySession.saveOrUpdate(auditEntitiesConfiguration.getRevisionInfoEntityName(), currentRevision);
				}
				temporarySession.save(auditEntityName, mergeAuditData);
				temporarySession.remove(temporarySession.load(auditEntityName, previousAuditDataId));
				temporarySession.flush();

				mergeInfo = Pair.make(MergeKind.MERGE_BY_UPDATE_FORWARDS, previousRevision);
			}
			else {
				attachIdToAuditData(mergeAuditData, temporarySession, entityConfiguration.getIdMapper(), entityId, previousRevision);
				setRevisionTimestamp(previousRevision, getRevisionTimestamp(currentRevision));

				temporarySession.clear();
				temporarySession.update(auditEntitiesConfiguration.getRevisionInfoEntityName(), previousRevision);
				temporarySession.update(auditEntityName, mergeAuditData);
				temporarySession.flush();

				mergeInfo = Pair.make(MergeKind.MERGE_BY_UPDATE_BACKWARDS, previousRevision);
			}
			mergeInfoByEntityByTransaction.computeIfAbsent(transaction, ignore -> new HashMap<>()).put(entityNameWithId, mergeInfo);

			// Remove (or just detach) the audit-entity that would normally be saved.
			// Using remove instead of detach in case the entity is already persisted.
			// Remove works for both persistent and transient entities.
			if (session.contains(auditEntityName, currentAuditData)) {
				session.remove(currentAuditData);
			}
			/* ==================== REVISION MERGE END ==================== */
		}
	}

	private void defaultPerform(
			Session session,
			String entityName,
			Serializable entityId,
			Object currentAuditData,
			Object currentRevision,
			Transaction transaction,
			Pair<String, Object> entityNameWithId
	) {
		super.perform(session, entityName, auditEntitiesConfiguration, entityId, currentAuditData, currentRevision);
		mergeInfoByEntityByTransaction.computeIfAbsent(transaction, ignore -> new HashMap<>()).put(entityNameWithId, Pair.make(MergeKind.NO_MERGE, null));
		session.flush(); // flush so that temporary sessions can see the revision info entity
	}

	@Override
	public void performCollectionChange(
			Session session,
			String entityName,
			String propertyName,
			AuditEntitiesConfiguration auditEntitiesConfiguration,
			PersistentCollectionChangeData persistentCollectionChangeData,
			Object revision
	) {
		final Object owningEntityId = getReferencingEntityId(entityName, propertyName, persistentCollectionChangeData);
		final Transaction transaction = session.getTransaction();
		final Map<Pair<String, Object>, Pair<MergeKind, Object>> mergeInfoByEntity = mergeInfoByEntityByTransaction.get(transaction);
		if (mergeInfoByEntity == null) {
			throw new RuntimeException("Collection change before parent change");
		}

		final Pair<MergeKind, Object> mergeInfo = mergeInfoByEntity.get(Pair.make(entityName, owningEntityId));
		if (mergeInfo == null) {
			throw new RuntimeException("Collection change before parent change");
		}

		if (mergeInfo.getFirst() == MergeKind.NO_MERGE) {
			super.performCollectionChange(session, entityName, propertyName, auditEntitiesConfiguration, persistentCollectionChangeData, revision);
			return;
		}

		try (Session temporarySession = createTemporarySession(session)) {
			final HashMap<?, ?> collectionOriginalId = (HashMap<?, ?>) persistentCollectionChangeData.getData()
					.get(auditEntitiesConfiguration.getOriginalIdPropName());
			final HashMap<Object, Object> targetAuditDataId = new HashMap<>(collectionOriginalId);
			targetAuditDataId.put(auditEntitiesConfiguration.getRevisionFieldName(), mergeInfo.getSecond());

			if (mergeInfo.getFirst() == MergeKind.MERGE_BY_UNDO) {
				// If we are undoing just remove the audit at the target revision
				final Object targetAuditData = temporarySession.get(persistentCollectionChangeData.getEntityName(), targetAuditDataId);
				if (targetAuditData == null) {
					throw new RuntimeException("A revision was invalidly merged.");
				}

				temporarySession.remove(targetAuditData);
				temporarySession.flush();
			}
			else if (mergeInfo.getFirst() == MergeKind.MERGE_BY_UPDATE_BACKWARDS) {
				final RevisionType currentRevisionType = getRevisionTypeFromAuditData(persistentCollectionChangeData.getData());
				final HashMap<String, Object> currentAuditBackPorted = new HashMap<>(persistentCollectionChangeData.getData());
				currentAuditBackPorted.put(auditEntitiesConfiguration.getOriginalIdPropName(), targetAuditDataId);
				switch (currentRevisionType) {
					case ADD:
						// Just save the current audit but at the target revision.
						temporarySession.save(persistentCollectionChangeData.getEntityName(), currentAuditBackPorted);
						break;
					case MOD:
						// 1: If no audit exists at the target revision just save the current audit at the target revision.
						// 2: If a MOD audit exists at the target revision just override it.
						// 3: If an ADD audit exists at the target revision override it but with revision type ADD (can this even happen?).
						final Object targetAuditMod = temporarySession.get(persistentCollectionChangeData.getEntityName(), targetAuditDataId);
						if (targetAuditMod == null) {
							temporarySession.save(persistentCollectionChangeData.getEntityName(), currentAuditBackPorted);
						}
						else {
							if (getRevisionTypeFromAuditData(targetAuditMod) == RevisionType.ADD) { // case 3
								currentAuditBackPorted.put(auditEntitiesConfiguration.getRevisionTypePropName(), RevisionType.ADD);
							}
							temporarySession.update(persistentCollectionChangeData.getEntityName(), currentAuditBackPorted);
						}
						break;
					case DEL:
						// 1: If no previous audit exists or it is a MOD audit, override/save the current audit at the target revision.
						// 2: If the previous audit is an ADD audit just delete it.
						final Object targetAuditDel = temporarySession.get(persistentCollectionChangeData.getEntityName(), targetAuditDataId);
						if (targetAuditDel == null) {
							temporarySession.save(persistentCollectionChangeData.getEntityName(), currentAuditBackPorted);
						}
						else if (getRevisionTypeFromAuditData(targetAuditDel) != RevisionType.ADD) {
							temporarySession.update(persistentCollectionChangeData.getEntityName(), currentAuditBackPorted);
						}
						else { // case 2
							temporarySession.remove(targetAuditDel);
						}
						break;
				}
				temporarySession.flush();
			}
			else if (mergeInfo.getFirst() == MergeKind.MERGE_BY_UPDATE_FORWARDS) {
				final RevisionType currentRevisionType = getRevisionTypeFromAuditData(persistentCollectionChangeData.getData());
				switch (currentRevisionType) {
					case ADD:
						// Just save the current audit as there is no previous revision.
						temporarySession.save(persistentCollectionChangeData.getEntityName(), persistentCollectionChangeData.getData());
						break;
					case MOD:
						// We always save the current audit.
						// 1: If there is no previous audit there's no need to do anything else.
						// 2: If the previous audit is a MOD audit we can just delete it.
						// 3: If the previous audit is an ADD audit we have to delete it and change our current audit type to ADD.
						final Object targetAuditMod = temporarySession.get(persistentCollectionChangeData.getEntityName(), targetAuditDataId);
						if (targetAuditMod == null) { // case 1
							temporarySession.save(persistentCollectionChangeData.getEntityName(), persistentCollectionChangeData.getData());
						}
						else { // case 2 + 3
							temporarySession.remove(targetAuditMod);
							final Map<String, Object> currentAudit;
							if (getRevisionTypeFromAuditData(targetAuditMod) == RevisionType.MOD) { // case 2
								currentAudit = persistentCollectionChangeData.getData();
							}
							else { // case 3
								currentAudit = new HashMap<>(persistentCollectionChangeData.getData());
								currentAudit.put(auditEntitiesConfiguration.getRevisionTypePropName(), RevisionType.ADD);
							}
							temporarySession.save(persistentCollectionChangeData.getEntityName(), currentAudit);
						}
						break;
					case DEL:
						// We always save the current audit.
						// 1: If there is no previous audit just save the current audit
						// 2: If the previous audit is a MOD audit we can just delete it and save the current audit.
						// 3: If the previous audit is an ADD audit we have to delete it there's no need to save the current audit.
						final Object targetAuditDel = temporarySession.get(persistentCollectionChangeData.getEntityName(), targetAuditDataId);
						if (targetAuditDel == null) { // case 1
							temporarySession.save(persistentCollectionChangeData.getEntityName(), persistentCollectionChangeData.getData());
						}
						else { // case 2 + 3
							temporarySession.remove(targetAuditDel);
							if (getRevisionTypeFromAuditData(targetAuditDel) == RevisionType.MOD) { // case 2
								temporarySession.save(persistentCollectionChangeData.getEntityName(), persistentCollectionChangeData.getData());
							}
						}
						break;
				}
				temporarySession.flush();
			}
		}
	}

	private Object getReferencingEntityId(String referencingEntityName, String propertyName, PersistentCollectionChangeData changeData) {
		final ExtendedPropertyMapper entityPropertyMapper = entitiesConfigurations.get(referencingEntityName).getPropertyMapper();
		final PropertyData collectionPropertyData = entityPropertyMapper.getPropertyDatas().get(propertyName);
		final AbstractCollectionMapper<?> collectionPropertyMapper = tryCastNonNull(
				entityPropertyMapper.getProperties().get(collectionPropertyData),
				AbstractCollectionMapper.class,
				"collection mapper");

		final HashMap<?, ?> collectionOriginalId = (HashMap<?, ?>) changeData.getData()
				.get(auditEntitiesConfiguration.getOriginalIdPropName());
		return collectionPropertyMapper.getReferencingIdData().getPrefixedMapper().mapToIdFromMap(collectionOriginalId);
	}

	private boolean isForwardMergeNecessary(EntityConfiguration entityConfiguration, Map<String, Object> auditData) {
		for (PropertyData propertyData : entityConfiguration.getPropertyMapper().getProperties().keySet()) {
			final RelationDescription relationDescription = entityConfiguration.getRelationDescription(propertyData.getName());
			if (relationDescription != null) {
				final Boolean columnWasModified = tryCast(auditData.get(propertyData.getModifiedFlagPropertyName()), Boolean.class, "modified flag");
				if (Boolean.TRUE.equals(columnWasModified)) {
					final IdMapper relationIdMapper = relationDescription.getIdMapper(); // null if relation uses middle table
					if (relationIdMapper == null || (relationIdMapper.mapToIdFromMap(auditData) != null)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	private Instant getRevisionTimestamp(Object revision) {
		final Object revisionTimestamp = revisionInfoTimestampGetter.get(revision);
		final Instant revisionTimestampInstant;
		if (revisionInfoTimestampIsDate) {
			revisionTimestampInstant = tryCastNonNull(revisionTimestamp, Date.class, "revision timestamp").toInstant();
		}
		else {
			revisionTimestampInstant = Instant.ofEpochMilli(tryCastNonNull(revisionTimestamp, Long.class, "revision timestamp"));
		}
		return revisionTimestampInstant;
	}

	private void setRevisionTimestamp(Object revision, Instant timestamp) {
		final Object value;
		if (revisionInfoTimestampIsDate) {
			value = Date.from(timestamp);
		}
		else {
			value = timestamp.toEpochMilli();
		}
		revisionInfoTimestampSetter.set(revision, value, null); // There's no implementation that uses the factory parameter.
	}

	private AbstractEntityPersister getPersisterFromEntityName(SessionImplementor session, String entityName) {
		final EntityPersister persister = session.getFactory().getMetamodel().entityPersister(entityName);
		return tryCast(persister, AbstractEntityPersister.class, "entity persister");
	}

	private RevisionType getRevisionTypeFromAuditData(Object auditData) {
		final Map<?, ?> mapData = MergingAuditStrategy.tryCast(auditData, Map.class, "audit data");
		return tryCastNonNull(
				mapData.get(auditEntitiesConfiguration.getRevisionTypePropName()), RevisionType.class, "revision type"
		);
	}

	private Class<?> getEntityClass(String entityName) {
		try {
			return Class.forName(entitiesConfigurations.get(entityName).getEntityClassName());
		}
		catch (ClassNotFoundException e) {
			throw new RuntimeException("Failed to fetch entity class from configuration.", e);
		}
	}

	private boolean isRevisionInMergeableTimeframe(Object currentRevision, Object previousRevision, EntityConfiguration entityConfiguration) {
		final long timeoutSeconds = entityConfiguration.getMergeTimeoutSeconds();
		if (timeoutSeconds <= 0L) {
			return true;
		}

		final Instant currentRevisionTimestamp = getRevisionTimestamp(currentRevision);
		final Instant previousRevisionTimestamp = getRevisionTimestamp(previousRevision);
		return currentRevisionTimestamp.isBefore(previousRevisionTimestamp.plusSeconds(timeoutSeconds));
	}

	// Pair(Object previousRevision, Object entityBeforePreviousRevision)
	private Pair<Object, Object> findPreviousRevision(AuditReader auditReader, Class<?> entityClass, String entityName, Object entityId) {
		final List<?> latestTwoRevisionsList = auditReader.createQuery()
				.forRevisionsOfEntity(entityClass, entityName, false, false)
				.add(AuditEntity.id().eq(entityId))
				.addOrder(AuditEntity.revisionNumber().desc())
				.setMaxResults(2)
				.getResultList();
		if (latestTwoRevisionsList.size() == 0) {
			return Pair.make(null, null);
		}
		else if (latestTwoRevisionsList.size() == 1) {
			Object[] previousRevisionTriplet = (Object[]) latestTwoRevisionsList.get(0); // this has to be an ADD audit
			return Pair.make(previousRevisionTriplet[1], null); // there is no state before the ADD audit
		}
		else {
			Object[] previousRevisionTriplet = (Object[]) latestTwoRevisionsList.get(0);
			Object[] prePreviousRevisionTriplet = (Object[]) latestTwoRevisionsList.get(1);

			final RevisionType previousRevisionType = tryCast(previousRevisionTriplet[2], RevisionType.class, "revision type");
			final RevisionType prePreviousRevisionType = tryCast(prePreviousRevisionTriplet[2], RevisionType.class, "revision type");
			if (previousRevisionType == RevisionType.ADD && prePreviousRevisionType == RevisionType.MOD) {
				// TODO: do we actually need to check for this?
				final Object[] swapHelper = previousRevisionTriplet;
				previousRevisionTriplet = prePreviousRevisionTriplet;
				prePreviousRevisionTriplet = swapHelper;
			}

			return Pair.make(previousRevisionTriplet[1], prePreviousRevisionTriplet[0]);
		}
	}

	private void attachIdToAuditData(Map<String, Object> auditData, Session session, IdMapper idMapper, Object entityId, Object revision) {
		final Map<String, Object> auditDataId = new HashMap<>();
		auditDataId.put(auditEntitiesConfiguration.getRevisionFieldName(), revision);
		idMapper.mapToMapFromId(session, auditDataId, entityId);
		auditData.put(auditEntitiesConfiguration.getRevisionTypePropName(), RevisionType.MOD);
		auditData.put(auditEntitiesConfiguration.getOriginalIdPropName(), auditDataId);
	}

	// nullable
	private Map<String, Object> createAuditEntityData(
			SessionImplementor session,
			String entityName,
			EntityTuplizer entityTuplizer,
			Object beforePreviousRevisionEntity,
			Object currentEntity,
			Object entityId
	) {
		final Map<String, Object> auditData = new HashMap<>();
		final ExtendedPropertyMapper entityPropertyMapper = entitiesConfigurations.get(entityName).getPropertyMapper();

		boolean didChange = false;
		for (Map.Entry<PropertyData, PropertyMapper> propertyEntry : entityPropertyMapper.getProperties().entrySet()) {
			final PropertyData propertyData = propertyEntry.getKey();
			final PropertyMapper propertyMapper = propertyEntry.getValue();

			final Object propertyBeforePreviousRevision = entityTuplizer.getPropertyValue(beforePreviousRevisionEntity, propertyData.getName());
			final Object propertyCurrent = entityTuplizer.getPropertyValue(currentEntity, propertyData.getName());
			didChange |= propertyMapper.mapToMapFromEntity(session, auditData, propertyCurrent, propertyBeforePreviousRevision);

			// For collections PropertyMapper::mapToMapFromEntity always returns false, but we need to know if the collection changed.
			if (propertyMapper instanceof AbstractCollectionMapper<?>) {
				final AbstractCollectionMapper<?> collectionPropertyMapper = (AbstractCollectionMapper<?>) propertyMapper;
				final PersistentCollection propertyCurrentPerCol = tryCast(propertyCurrent, PersistentCollection.class, "collection property");
				final Serializable propertyBeforePreviousRevisionSer = tryCast(propertyBeforePreviousRevision, Serializable.class, "collection property");
				final Serializable entityIdSer = tryCast(entityId, Serializable.class, "entity id");

				final List<?> collectionChanges = collectionPropertyMapper.mapCollectionChanges(
						session, propertyData.getName(), propertyCurrentPerCol, propertyBeforePreviousRevisionSer, entityIdSer
				);
				final boolean isModified = !collectionChanges.isEmpty();

				didChange |= isModified;
				auditData.put(propertyData.getModifiedFlagPropertyName(), isModified);
			}
			else {
				propertyMapper.mapModifiedFlagsToMapFromEntity(session, auditData, propertyBeforePreviousRevision, propertyCurrent);
			}
		}

		return didChange ? auditData : null;
	}

	private enum MergeKind {
		NO_MERGE,
		MERGE_BY_UNDO,
		MERGE_BY_UPDATE_BACKWARDS,
		MERGE_BY_UPDATE_FORWARDS
	}

}
