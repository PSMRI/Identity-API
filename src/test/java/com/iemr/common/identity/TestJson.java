package com.iemr.common.identity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.json.JSONObject;

/**
 * Helpers for asserting on the {@code OutputResponse} JSON that the controllers return.
 * {@code org.json} declares {@code JSONException} as checked, so parsing is wrapped here to keep
 * the test bodies free of {@code throws} clauses.
 */
public final class TestJson {

	private TestJson() {
	}

	public static JSONObject parse(String raw) {
		try {
			return new JSONObject(raw);
		} catch (Exception e) {
			throw new IllegalArgumentException("not valid JSON: " + raw, e);
		}
	}

	public static String str(JSONObject json, String key) {
		try {
			return json.getString(key);
		} catch (Exception e) {
			throw new IllegalArgumentException("missing string key " + key + " in " + json, e);
		}
	}

	public static int statusCode(String raw) {
		try {
			return parse(raw).getInt("statusCode");
		} catch (Exception e) {
			throw new IllegalArgumentException("missing statusCode in " + raw, e);
		}
	}

	/** Asserts a 200 OutputResponse whose serialized data contains {@code dataFragment}. */
	public static void assertSuccess(String raw, String dataFragment) {
		JSONObject json = parse(raw);
		assertEquals(200, statusCode(raw), () -> "expected success but got: " + raw);
		assertTrue(json.toString().contains(dataFragment),
				() -> "expected data to contain '" + dataFragment + "' but was: " + raw);
	}

	/** Asserts a failing OutputResponse whose errorMessage contains {@code messageFragment}. */
	public static void assertFailure(String raw, int expectedStatusCode, String messageFragment) {
		JSONObject json = parse(raw);
		assertEquals(expectedStatusCode, statusCode(raw), () -> "unexpected status for: " + raw);
		assertTrue(str(json, "errorMessage").contains(messageFragment),
				() -> "expected errorMessage to contain '" + messageFragment + "' but was: " + raw);
	}

	public static void assertFailure(String raw, String messageFragment) {
		assertFailure(raw, 5000, messageFragment);
	}
}
