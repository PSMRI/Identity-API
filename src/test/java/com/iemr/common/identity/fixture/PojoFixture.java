/*
* AMRIT – Accessible Medical Records via Integrated Technology
* Integrated EHR (Electronic Health Records) Solution
*
* Copyright (C) "Piramal Swasthya Management and Research Institute"
*
* This file is part of AMRIT.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program.  If not, see https://www.gnu.org/licenses/.
*/
package com.iemr.common.identity.fixture;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reflection-based test fixture builder.
 *
 * <p>
 * Identity-API leans heavily on generated MapStruct mappers and on wide JPA
 * entities whose only logic is a long chain of {@code if (value != null)}
 * guards. Hand-writing a fully populated {@code IdentityEditDTO} graph for
 * every mapper test would run to thousands of lines and rot on the first
 * schema change, so tests build their inputs here instead.
 *
 * <ul>
 * <li>{@link #populate(Class)} returns an instance with every writable
 * property set to a deterministic non-null value, recursing into nested
 * POJOs and collections. This drives the "value present" side of the
 * mappers' null guards.</li>
 * <li>{@link #blank(Class)} returns a bare instance so the same mapper call
 * exercises the "value absent" side.</li>
 * <li>{@link #exerciseAccessors(Object)} reads every property back, which
 * covers the generated getters on the entity and DTO layers.</li>
 * </ul>
 */
public final class PojoFixture {

	/** Depth cap: entities reference each other, so the graph must be bounded. */
	private static final int DEFAULT_DEPTH = 4;

	private PojoFixture() {
	}

	/** Creates an instance with no properties set. */
	public static <T> T blank(Class<T> type) {
		return instantiate(type);
	}

	/** Creates an instance with every writable property set to a non-null value. */
	public static <T> T populate(Class<T> type) {
		return populate(type, DEFAULT_DEPTH);
	}

	/** Creates a populated instance, recursing at most {@code depth} levels. */
	public static <T> T populate(Class<T> type, int depth) {
		T instance = instantiate(type);
		fill(instance, depth);
		return instance;
	}

	/** Sets every writable property on an existing instance. */
	public static void fill(Object instance, int depth) {
		for (Method setter : instance.getClass().getMethods()) {
			if (!isSetter(setter)) {
				continue;
			}
			Object value = valueFor(setter.getParameterTypes()[0], setter.getGenericParameterTypes()[0], depth);
			if (value == null) {
				continue;
			}
			try {
				setter.invoke(instance, value);
			} catch (ReflectiveOperationException | IllegalArgumentException e) {
				// A property we cannot supply is simply left at its default.
			}
		}
	}

	/**
	 * Invokes every no-argument getter and returns how many were read. Used to
	 * cover accessor-only classes without asserting on each field by hand.
	 */
	public static int exerciseAccessors(Object instance) {
		int read = 0;
		for (Method getter : instance.getClass().getMethods()) {
			if (!isGetter(getter)) {
				continue;
			}
			try {
				getter.invoke(instance);
				read++;
			} catch (ReflectiveOperationException | RuntimeException e) {
				// Derived getters may reject the fixture's values; skip them.
			}
		}
		return read;
	}

	/**
	 * Populates an instance, reads every property back and also exercises
	 * {@code toString}/{@code hashCode}/{@code equals} where Lombok generated
	 * them. Returns the populated instance so callers can assert on it.
	 */
	public static <T> T roundTrip(Class<T> type) {
		T populated = populate(type);
		exerciseAccessors(populated);
		exerciseObjectMethods(populated, populate(type), blank(type));
		return populated;
	}

	private static void exerciseObjectMethods(Object first, Object second, Object third) {
		try {
			first.toString();
			first.hashCode();
			first.equals(first);
			first.equals(second);
			first.equals(third);
			first.equals(null);
			first.equals("not the same type");
		} catch (RuntimeException e) {
			// Generated equals/hashCode can trip over lazy fields; not the subject
			// of the test.
		}
	}

	private static boolean isSetter(Method method) {
		return method.getName().startsWith("set") && method.getParameterCount() == 1
				&& !Modifier.isStatic(method.getModifiers()) && method.getDeclaringClass() != Object.class;
	}

	private static boolean isGetter(Method method) {
		if (method.getParameterCount() != 0 || Modifier.isStatic(method.getModifiers())
				|| method.getDeclaringClass() == Object.class || method.getReturnType() == void.class) {
			return false;
		}
		String name = method.getName();
		return name.startsWith("get") || name.startsWith("is");
	}

	@SuppressWarnings("unchecked")
	private static <T> T instantiate(Class<T> type) {
		try {
			Constructor<T> constructor = type.getDeclaredConstructor();
			constructor.setAccessible(true);
			return constructor.newInstance();
		} catch (ReflectiveOperationException e) {
			// Fall back to the widest constructor, supplying values for its
			// parameters.
			Constructor<?>[] constructors = type.getDeclaredConstructors();
			Constructor<?> widest = null;
			for (Constructor<?> candidate : constructors) {
				if (widest == null || candidate.getParameterCount() > widest.getParameterCount()) {
					widest = candidate;
				}
			}
			if (widest == null) {
				throw new IllegalArgumentException("No usable constructor for " + type.getName(), e);
			}
			Object[] args = new Object[widest.getParameterCount()];
			for (int i = 0; i < args.length; i++) {
				args[i] = valueFor(widest.getParameterTypes()[i], widest.getGenericParameterTypes()[i], 1);
			}
			try {
				widest.setAccessible(true);
				return (T) widest.newInstance(args);
			} catch (ReflectiveOperationException nested) {
				throw new IllegalArgumentException("Cannot instantiate " + type.getName(), nested);
			}
		}
	}

	private static Object valueFor(Class<?> type, Type genericType, int depth) {
		if (type == String.class) {
			return "test";
		}
		if (type == Integer.class || type == int.class) {
			return Integer.valueOf(1);
		}
		if (type == Long.class || type == long.class) {
			return Long.valueOf(1L);
		}
		if (type == Short.class || type == short.class) {
			return Short.valueOf((short) 1);
		}
		if (type == Byte.class || type == byte.class) {
			return Byte.valueOf((byte) 1);
		}
		if (type == Double.class || type == double.class) {
			return Double.valueOf(1d);
		}
		if (type == Float.class || type == float.class) {
			return Float.valueOf(1f);
		}
		if (type == Boolean.class || type == boolean.class) {
			return Boolean.TRUE;
		}
		if (type == Character.class || type == char.class) {
			return Character.valueOf('Y');
		}
		if (type == BigInteger.class) {
			return BigInteger.ONE;
		}
		if (type == BigDecimal.class) {
			return BigDecimal.ONE;
		}
		if (type == Timestamp.class) {
			return new Timestamp(FIXED_MILLIS);
		}
		if (type == Date.class) {
			return new Date(FIXED_MILLIS);
		}
		if (type == java.util.Date.class) {
			return new java.util.Date(FIXED_MILLIS);
		}
		if (type == LocalDate.class) {
			return LocalDate.of(1990, 1, 1);
		}
		if (type == LocalDateTime.class) {
			return LocalDateTime.of(1990, 1, 1, 0, 0);
		}
		if (type == byte[].class) {
			return new byte[] { 1, 2, 3 };
		}
		if (type.isEnum()) {
			Object[] constants = type.getEnumConstants();
			return constants.length > 0 ? constants[0] : null;
		}
		if (type == Object.class) {
			return "test";
		}
		if (Collection.class.isAssignableFrom(type)) {
			return collectionFor(type, genericType, depth);
		}
		if (Map.class.isAssignableFrom(type)) {
			return mapFor(type, genericType, depth);
		}
		if (depth <= 1 || type.isInterface() || Modifier.isAbstract(type.getModifiers()) || type.isArray()
				|| !type.getName().startsWith("com.iemr")) {
			return null;
		}
		try {
			return populate(type, depth - 1);
		} catch (RuntimeException e) {
			return null;
		}
	}

	private static final long FIXED_MILLIS = 631152000000L; // 1990-01-01T00:00:00Z

	private static Object collectionFor(Class<?> type, Type genericType, int depth) {
		Collection<Object> collection = Set.class.isAssignableFrom(type)
				? (type == LinkedHashSet.class ? new LinkedHashSet<>() : new HashSet<>())
				: new ArrayList<>();
		Class<?> elementType = typeArgument(genericType, 0);
		if (elementType != null && depth > 1) {
			Object element = valueFor(elementType, elementType, depth - 1);
			if (element != null) {
				collection.add(element);
			}
		}
		return collection;
	}

	private static Object mapFor(Class<?> type, Type genericType, int depth) {
		Map<Object, Object> map = type == LinkedHashMap.class ? new LinkedHashMap<>() : new HashMap<>();
		Class<?> keyType = typeArgument(genericType, 0);
		Class<?> valueType = typeArgument(genericType, 1);
		if (keyType != null && valueType != null && depth > 1) {
			Object key = valueFor(keyType, keyType, depth - 1);
			Object value = valueFor(valueType, valueType, depth - 1);
			if (key != null && value != null) {
				map.put(key, value);
			}
		}
		return map;
	}

	private static Class<?> typeArgument(Type genericType, int index) {
		if (!(genericType instanceof ParameterizedType)) {
			return null;
		}
		Type[] arguments = ((ParameterizedType) genericType).getActualTypeArguments();
		if (index >= arguments.length) {
			return null;
		}
		Type argument = arguments[index];
		if (argument instanceof Class<?>) {
			return (Class<?>) argument;
		}
		if (argument instanceof ParameterizedType
				&& ((ParameterizedType) argument).getRawType() instanceof Class<?>) {
			return (Class<?>) ((ParameterizedType) argument).getRawType();
		}
		return null;
	}

	/**
	 * Builds a value suitable for a method parameter of the given declared type,
	 * resolving element types for collection parameters. Returns {@code null} when
	 * the type is one the fixture cannot supply.
	 */
	public static Object argument(Class<?> type, Type genericType) {
		return valueFor(type, genericType, DEFAULT_DEPTH);
	}

	/** Convenience for tests that need a single-element list of populated items. */
	public static <T> List<T> populatedList(Class<T> type) {
		List<T> list = new ArrayList<>();
		list.add(populate(type));
		return list;
	}
}
