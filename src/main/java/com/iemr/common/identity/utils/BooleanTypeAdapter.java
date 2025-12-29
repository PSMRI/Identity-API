package com.iemr.common.identity.utils;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

public class BooleanTypeAdapter extends TypeAdapter<Boolean> {

		@Override
		public void write(JsonWriter out, Boolean value) throws IOException {
			if (value == null) {
				out.nullValue();
			} else {
				out.value(value);
			}
		}

		@Override
		public Boolean read(JsonReader in) throws IOException {
			JsonToken token = in.peek();

			if (token == JsonToken.BOOLEAN) {
				return in.nextBoolean();
			}
			if (token == JsonToken.STRING) {
				return Boolean.parseBoolean(in.nextString());
			}
			if (token == JsonToken.NUMBER) {
				return in.nextInt() == 1;
			}
			in.skipValue();
			return false;
		}
	}
