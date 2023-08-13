package com.jslib.etl.meta;

/**
 * Meta-data for external services, also known as resources.
 * 
 * @author Iulian Rotaru
 */
public class ServiceMeta {
	private final ServiceType type;
	private final MetaProperties properties;

	public ServiceMeta(ServiceType type) {
		this.type = type;
		this.properties = new MetaProperties();
	}

	public ServiceType getType() {
		return type;
	}

	public MetaProperties getProperties() {
		return properties;
	}

	void putProperty(String name, String value) {
		properties.put(name, value);
	}

	@Override
	public String toString() {
		return "ServiceMeta [type=" + type + ", properties=" + properties + "]";
	}
}
