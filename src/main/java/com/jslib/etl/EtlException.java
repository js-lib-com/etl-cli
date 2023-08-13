package com.jslib.etl;

public class EtlException extends RuntimeException {
	private static final long serialVersionUID = -5140975035615518445L;

	public EtlException() {
		super();
	}

	public EtlException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public EtlException(String message, Throwable cause) {
		super(message, cause);
	}

	public EtlException(String message) {
		super(message);
	}

	public EtlException(String message, Object... args) {
		super(String.format(message, args));
	}

	public EtlException(Throwable cause) {
		super(cause);
	}
}
