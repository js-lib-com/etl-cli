package com.jslib.etl.meta;

import java.io.IOException;
import java.io.Reader;

public interface IParser {

	ProjectMeta parse(Reader reader) throws IOException;

}
