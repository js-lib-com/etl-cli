package com.jslib.etl.impl;

import com.jslib.etl.meta.ServiceMeta;

@FunctionalInterface interface ServiceFactory {
	void create(ServiceMeta meta);
}