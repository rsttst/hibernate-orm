/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.envers;

public @interface AuditMerge {

	/**
	 * Whether revisions of the entity should be merged. Defaults to {@literal true}.
	 */
	boolean mergeable() default true;

	/**
	 * The timeframe in seconds in which revisions of this entity are merged if mergeable is true. Defaults to {@literal 300} (=5m).
	 * If this value is zero or a negative number it is interpreted as an infinite timespan..
	 */
	long timeoutSeconds() default 300L;

}
