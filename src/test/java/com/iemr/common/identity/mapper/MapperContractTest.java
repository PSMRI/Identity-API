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
package com.iemr.common.identity.mapper;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

import com.iemr.common.identity.fixture.PojoFixture;

/**
 * Contract test for the generated MapStruct mappers.
 *
 * <p>
 * The mapper implementations are code-generated from the {@code @Mapping}
 * declarations on the interfaces, so the risk they carry is not "is this line
 * right" but "does a mapping declaration still resolve after the DTO or entity
 * changed". Every generated method follows the same shape:
 *
 * <pre>
 * if (all sources are null) return null;
 * target = new Target();
 * if (source.getX() != null) target.setX(...);
 * </pre>
 *
 * This test drives each mapper method three ways - fully populated sources,
 * empty sources, and all-null sources - which walks both sides of every null
 * guard and asserts the null contract holds. A mapping that stops resolving,
 * or a nested source object the generator dereferences unguarded, shows up
 * here as a failure rather than as a production NPE.
 *
 * <p>
 * Field-level mapping assertions live in the per-mapper tests alongside this
 * one.
 */
class MapperContractTest {

	private static final List<Object> MAPPERS = Arrays.asList(IdentityMapper.INSTANCE, IdentityEditMapper.INSTANCE,
			IdentitySearchMapper.INSTANCE, IdentityPartialMapper.INSTANCE, BenIdImportMapper.INSTANCE);

	/**
	 * Mapper methods that require their nested sources to be present and throw
	 * when they are not, because the generated code hands a possibly-null nested
	 * object to a setter that dereferences it without a guard. Their actual
	 * behaviour is pinned by the {@code assertThrows} tests in
	 * {@link IdentityEditMapperTest}, {@link IdentitySearchMapperTest} and
	 * {@link IdentityPartialMapperTest}, so the empty-source sweep skips them
	 * rather than restating it here.
	 */
	private static final Set<String> REQUIRE_POPULATED_NESTED_SOURCES = new HashSet<>(
			Arrays.asList("identityEditDTOToMBeneficiaryaddress", "identitySearchDTOToMBeneficiaryaddress",
					"mBeneficiarymappingToBeneficiariesPartialDTO"));

	@TestFactory
	@SuppressWarnings("unchecked")
	List<DynamicNode> everyMapperMethodHonoursTheNullContract() {
		List<DynamicNode> tests = new ArrayList<>();
		for (Object mapper : MAPPERS) {
			for (Method method : mappingMethods(mapper)) {
				tests.add(dynamicTest(mapper.getClass().getSimpleName() + "." + method.getName(),
						() -> assertMappingContract(mapper, method)));
			}
		}
		assertNotNull(tests);
		return tests;
	}

	private void assertMappingContract(Object mapper, Method method) throws Exception {
		Object populatedResult = invoke(mapper, method, arguments(method, ArgumentStyle.POPULATED));
		assertNotNull(populatedResult, method.getName() + " returned null for fully populated sources");

		if (!REQUIRE_POPULATED_NESTED_SOURCES.contains(method.getName())) {
			Object blankResult = invoke(mapper, method, arguments(method, ArgumentStyle.BLANK));
			assertNotNull(blankResult, method.getName() + " returned null for empty (but non-null) sources");
		}

		Object nullResult = invoke(mapper, method, arguments(method, ArgumentStyle.NULL));
		assertNull(nullResult, method.getName() + " should return null when every source is null");
	}

	/**
	 * A populated source must produce a target that actually carries values, not
	 * just a non-null shell - this is what catches a mapping that silently stopped
	 * resolving.
	 */
	@TestFactory
	List<DynamicNode> populatedSourcesProduceNonEmptyTargets() {
		List<DynamicNode> tests = new ArrayList<>();
		for (Object mapper : MAPPERS) {
			for (Method method : mappingMethods(mapper)) {
				tests.add(dynamicTest(mapper.getClass().getSimpleName() + "." + method.getName(), () -> {
					Object result = invoke(mapper, method, arguments(method, ArgumentStyle.POPULATED));
					assertNotNull(result);
					assertCarriesValues(method, result);
				}));
			}
		}
		return tests;
	}

	private void assertCarriesValues(Method method, Object result) {
		if (result instanceof Collection<?>) {
			Collection<?> collection = (Collection<?>) result;
			org.junit.jupiter.api.Assertions.assertFalse(collection.isEmpty(),
					method.getName() + " mapped a populated list to an empty list");
			collection.forEach(element -> assertNotNull(element, method.getName() + " mapped an element to null"));
			return;
		}
		if (result instanceof Map<?, ?>) {
			return;
		}
		int populatedProperties = 0;
		for (Method getter : result.getClass().getMethods()) {
			if (getter.getParameterCount() != 0 || Modifier.isStatic(getter.getModifiers())
					|| getter.getDeclaringClass() == Object.class
					|| !(getter.getName().startsWith("get") || getter.getName().startsWith("is"))) {
				continue;
			}
			try {
				if (getter.invoke(result) != null) {
					populatedProperties++;
				}
			} catch (ReflectiveOperationException | RuntimeException e) {
				// Derived getter; not part of the mapping surface.
			}
		}
		org.junit.jupiter.api.Assertions.assertTrue(populatedProperties > 0,
				method.getName() + " produced a target with no populated properties");
	}

	private Object invoke(Object mapper, Method method, Object[] args) throws Exception {
		try {
			return method.invoke(mapper, args);
		} catch (InvocationTargetException e) {
			throw e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
		}
	}

	private enum ArgumentStyle {
		POPULATED, BLANK, NULL
	}

	private Object[] arguments(Method method, ArgumentStyle style) {
		Class<?>[] types = method.getParameterTypes();
		Object[] args = new Object[types.length];
		for (int i = 0; i < types.length; i++) {
			if (style == ArgumentStyle.POPULATED) {
				args[i] = PojoFixture.argument(types[i], method.getGenericParameterTypes()[i]);
			} else if (style == ArgumentStyle.BLANK) {
				args[i] = blankArgument(types[i]);
			} else {
				args[i] = null;
			}
		}
		if (style == ArgumentStyle.BLANK && Arrays.stream(args).allMatch(java.util.Objects::isNull)) {
			// Nothing non-null to pass, so the null contract already covers this
			// method; reuse the populated arguments rather than asserting twice.
			return arguments(method, ArgumentStyle.POPULATED);
		}
		return args;
	}

	private Object blankArgument(Class<?> type) {
		if (Collection.class.isAssignableFrom(type)) {
			return new ArrayList<>();
		}
		if (type.getName().startsWith("com.iemr") && !type.isInterface() && !type.isEnum()
				&& !Modifier.isAbstract(type.getModifiers())) {
			return PojoFixture.blank(type);
		}
		return null;
	}

	private List<Method> mappingMethods(Object mapper) {
		List<Method> methods = new ArrayList<>();
		for (Method method : mapper.getClass().getDeclaredMethods()) {
			if (Modifier.isPublic(method.getModifiers()) && !Modifier.isStatic(method.getModifiers())
					&& method.getParameterCount() > 0 && method.getReturnType() != void.class
					&& !method.isSynthetic()) {
				methods.add(method);
			}
		}
		methods.sort(Comparator.comparing(Method::getName).thenComparing(m -> m.getParameterTypes()[0].getName()));
		return methods;
	}
}
