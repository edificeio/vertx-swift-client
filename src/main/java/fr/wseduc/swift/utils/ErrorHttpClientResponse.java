package fr.wseduc.swift.utils;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.net.NetSocket;

import java.util.List;

public class ErrorHttpClientResponse implements HttpClientResponse {

	private final int statusCode;
	private final String statusMessage;

	public ErrorHttpClientResponse(int statusCode, String statusMessage) {
		this.statusCode = statusCode;
		this.statusMessage = statusMessage;
	}

	@Override
	public HttpClientResponse exceptionHandler(Handler<Throwable> handler) {
		return null;
	}

	@Override
	public int statusCode() {
		return statusCode;
	}

	@Override
	public String statusMessage() {
		return statusMessage;
	}

	@Override
	public MultiMap headers() {
		return null;
	}

	@Override
	public MultiMap trailers() {
		return null;
	}

	@Override
	public List<String> cookies() {
		return null;
	}

	@Override
	public HttpClientResponse bodyHandler(Handler<Buffer> handler) {
		return null;
	}

	@Override
	public NetSocket netSocket() {
		return null;
	}

	@Override
	public HttpClientResponse endHandler(Handler<Void> handler) {
		return null;
	}

	@Override
	public HttpClientResponse dataHandler(Handler<Buffer> handler) {
		return null;
	}

	@Override
	public HttpClientResponse pause() {
		return null;
	}

	@Override
	public HttpClientResponse resume() {
		return null;
	}

}
