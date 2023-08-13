package com.jslib.etl.meta;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import com.jslib.converter.Converter;
import com.jslib.converter.ConverterRegistry;
import com.jslib.util.Params;
import com.jslib.util.Strings;

public class MetaProperties {
	private final Converter converter;
	private final Map<String, String> properties;
	private final Set<String> excludes;

	public MetaProperties() {
		this.converter = ConverterRegistry.getConverter();
		this.properties = new HashMap<>();
		this.excludes = new HashSet<>();
	}

	public void put(String name, String value) {
		Params.notNullOrEmpty(name, "Name");

		// ETL DSL allows space in property name from service configuration
		List<String> words = Strings.split(name, '-', ' ', '/', '\\');
		if (words.size() == 1) {
			properties.put(name, value);
			return;
		}

		StringBuilder propertyName = new StringBuilder();
		boolean first = true;
		for (String word : words) {
			if (first) {
				first = false;
				propertyName.append(word.toLowerCase());
				continue;
			}
			propertyName.append(Character.toUpperCase(word.charAt(0)));
			propertyName.append(word.substring(1).toLowerCase());
		}

		properties.put(propertyName.toString(), value);
	}

	public String get(String name) {
		return get(name, String.class, null);
	}

	public String get(String name, String defaultValue) {
		return get(name, String.class, defaultValue);
	}

	public <T> T get(String name, Class<T> type) {
		return get(name, type, null);
	}

	public <T> T get(String name, Class<T> type, T defaultValue) {
		Params.notNullOrEmpty(name, "Name");
		Params.notNull(type, "Type");
		excludes.add(name);
		T value = converter.asObject(properties.get(name), type);
		return value != null ? value : defaultValue;
	}

	public void forEach(BiConsumer<String, String> consumer) {
		for (String name : properties.keySet()) {
			if (!excludes.contains(name)) {
				consumer.accept(name, properties.get(name));
			}
		}
	}
}
