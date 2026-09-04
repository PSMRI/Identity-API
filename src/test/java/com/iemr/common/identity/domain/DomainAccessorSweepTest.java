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
package com.iemr.common.identity.domain;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.security.CodeSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.core.type.filter.RegexPatternTypeFilter;

import com.iemr.common.identity.fixture.PojoFixture;

/**
 * Structural sweep over the entity, DTO and data-model layers.
 *
 * <p>
 * Hibernate instantiates entities through a no-argument constructor and then
 * populates them through their accessors, and Jackson does the same for the DTOs
 * on the wire. Neither reports a class that has lost its default constructor,
 * gained a setter with no matching getter, or acquired an accessor that throws -
 * the failure surfaces at runtime as a mapping or deserialisation error instead.
 *
 * <p>
 * This test walks every class in those packages, sets each writable property,
 * reads it back, and exercises the generated {@code equals}/{@code hashCode}/
 * {@code toString}. Classes whose behaviour is more than a property bag have
 * their own tests - see {@link MBeneficiarydetailTest} and {@link PhoneTest}.
 */
class DomainAccessorSweepTest {

	private static final List<String> PACKAGES = Arrays.asList("com.iemr.common.identity.domain",
			"com.iemr.common.identity.dto", "com.iemr.common.identity.data");

	@TestFactory
	List<DynamicNode> everyModelClassRoundTripsItsProperties() {
		List<DynamicNode> tests = new ArrayList<>();
		for (Class<?> type : modelClasses()) {
			tests.add(dynamicTest(type.getSimpleName(), () -> {
				Object populated = PojoFixture.roundTrip(type);
				assertNotNull(populated, type.getName() + " could not be instantiated and populated");
				assertTrue(PojoFixture.exerciseAccessors(populated) > 0,
						type.getName() + " exposes no readable properties");
			}));
		}
		assertFalse(tests.isEmpty(), "the model packages should not be empty - has the scan path changed?");
		return tests;
	}

	private List<Class<?>> modelClasses() {
		ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
		scanner.addIncludeFilter(new RegexPatternTypeFilter(java.util.regex.Pattern.compile(".*")));
		scanner.addExcludeFilter(new AssignableTypeFilter(Throwable.class));
		List<Class<?>> classes = new ArrayList<>();
		for (String basePackage : PACKAGES) {
			for (BeanDefinition definition : scanner.findCandidateComponents(basePackage)) {
				String name = definition.getBeanClassName();
				if (name == null || name.contains("$")) {
					continue;
				}
				try {
					Class<?> type = Class.forName(name);
					if (!type.isEnum() && !type.isInterface() && isProductionClass(type)) {
						classes.add(type);
					}
				} catch (ClassNotFoundException e) {
					throw new IllegalStateException("Scanned class is not loadable: " + name, e);
				}
			}
		}
		classes.sort(java.util.Comparator.comparing(Class::getName));
		return classes;
	}

	/**
	 * The scan runs against the test classpath, which also contains the tests
	 * that live in these packages; only classes built from {@code src/main} are
	 * the subject here.
	 */
	private boolean isProductionClass(Class<?> type) {
		CodeSource codeSource = type.getProtectionDomain().getCodeSource();
		return codeSource != null && codeSource.getLocation().getPath().endsWith("/target/classes/");
	}
}
