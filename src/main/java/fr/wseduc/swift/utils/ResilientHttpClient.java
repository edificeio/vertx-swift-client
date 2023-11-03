package fr.wseduc.swift.utils;

import fr.wseduc.swift.storage.DefaultAsyncResult;
import io.vertx.core.*;
import io.vertx.core.http.*;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.SSLOptions;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class ResilientHttpClient implements HttpClient {

	private static final Logger log = LoggerFactory.getLogger(ResilientHttpClient.class);

	private final Vertx vertx;
	private HttpClient httpClient;
	private final int timeout;
	private final int threshold;
	private final long openDelay;
	private final URI uri;
	private final boolean keepAlive;
	private AtomicInteger errorsCount = new AtomicInteger(0);
	private AtomicBoolean closedCircuit = new AtomicBoolean(false);
	private Handler<HalfOpenResult> halfOpenHandler;

	public ResilientHttpClient(Vertx vertx, URI uri, boolean keepAlive, int timeout, int threshold, long openDelay) {
		this.vertx = vertx;
		this.timeout = timeout;
		this.threshold = threshold;
		this.openDelay = openDelay;
		this.uri = uri;
		this.keepAlive = keepAlive;
		reconfigure();
	}

	@Override
	public Future<WebSocket> webSocket(int port, String host, String requestURI) {
		return httpClient.webSocket(port, host, requestURI);
	}

	@Override
	public Future<WebSocket> webSocket(String host, String requestURI) {
		return httpClient.webSocket(host, requestURI);
	}

	@Override
	public Future<WebSocket> webSocket(String requestURI) {
		return httpClient.webSocket(requestURI);
	}

	@Override
	public Future<WebSocket> webSocket(WebSocketConnectOptions options) {
		return httpClient.webSocket(options);
	}

	@Override
	public Future<WebSocket> webSocketAbs(String url, MultiMap headers, WebsocketVersion version, List<String> subProtocols) {
		return httpClient.webSocketAbs(url, headers, version, subProtocols);
	}

	@Override
	public void webSocket(int port, String host, String requestURI, Handler<AsyncResult<WebSocket>> handler) {
		httpClient.webSocket(port, host, requestURI, handler);
	}

	@Override
	public void webSocket(String host, String requestURI, Handler<AsyncResult<WebSocket>> handler) {
		httpClient.webSocket(host, requestURI, handler);
	}

	@Override
	public void webSocket(String requestURI, Handler<AsyncResult<WebSocket>> handler) {
		httpClient.webSocket(requestURI, handler);
	}

	@Override
	public void webSocket(WebSocketConnectOptions options, Handler<AsyncResult<WebSocket>> handler) {
		httpClient.webSocket(options, handler);
	}

	@Override
	public void webSocketAbs(String url, MultiMap headers, WebsocketVersion version, List<String> subProtocols, Handler<AsyncResult<WebSocket>> handler) {
		httpClient.webSocketAbs(url, headers, version, subProtocols, handler);
	}

	@Override
	public HttpClient connectionHandler(Handler<HttpConnection> handler) {
		return httpClient.connectionHandler(handler);
	}


	@Override
	public Future<HttpClientRequest> request(RequestOptions options) {
		if (httpClient == null) {
			return Future.failedFuture("httpClient.null");
		}
		return httpClient.request(options)
				.map(this::preConfigureRequest);
	}

	@Override
	public void request(HttpMethod method, int port, String host, String requestURI, Handler<AsyncResult<HttpClientRequest>> handler) {
		if (httpClient == null) {
			handler.handle(new DefaultAsyncResult<>(new IllegalArgumentException("httpClient.null")));
		}
		httpClient.request(method, port, host, requestURI, e -> {
			if(e.succeeded()) {
				final HttpClientRequest req = preConfigureRequest(e.result());
				handler.handle(new DefaultAsyncResult<>(req));
			} else {
				handler.handle(e);
			}
		});
	}

	@Override
	public void request(HttpMethod method, String host, String requestURI, Handler<AsyncResult<HttpClientRequest>> handler) {
		if (httpClient == null) {
						handler.handle(new DefaultAsyncResult<>(new IllegalArgumentException("httpClient.null")));

		}
		httpClient.request(method, host, requestURI, e -> {
			if(e.succeeded()) {
				final HttpClientRequest req = preConfigureRequest(e.result());
				handler.handle(new DefaultAsyncResult<>(req));
			} else {
				handler.handle(e);
			}
		});
	}

	@Override
	public void request(HttpMethod method, String requestURI, Handler<AsyncResult<HttpClientRequest>> handler) {
		if (httpClient == null) {
			handler.handle(new DefaultAsyncResult<>(new IllegalArgumentException("httpClient.null")));
		}
		httpClient.request(method, requestURI, e -> {
			if(e.succeeded()) {
				final HttpClientRequest req = preConfigureRequest(e.result());
				handler.handle(new DefaultAsyncResult<>(req));
			} else {
				handler.handle(e);
			}
		});
	}

	@Override
	public Future<HttpClientRequest> request(HttpMethod method, String requestURI) {
		return httpClient.request(method, requestURI)
				.map(this::preConfigureRequest);
	}


	@Override
	public void request(RequestOptions options, Handler<AsyncResult<HttpClientRequest>> handler) {
		if (httpClient == null) {
			handler.handle(new DefaultAsyncResult<>(new IllegalArgumentException("httpClient.null")));
		}
		httpClient.request(options, e -> {
			if(e.succeeded()) {
				final HttpClientRequest req = preConfigureRequest(e.result());
				handler.handle(new DefaultAsyncResult<>(req));
			} else {
				handler.handle(e);
			}
		});
	}

	@Override
	public Future<HttpClientRequest> request(HttpMethod method, int port, String host, String requestURI) {
		return httpClient.request(method, port, host, requestURI)
				.map(this::preConfigureRequest);
	}

	@Override
	public Future<HttpClientRequest> request(HttpMethod method, String host, String requestURI) {
		return httpClient.request(method, host, requestURI)
				.map(this::preConfigureRequest);
	}

	@Override
	public HttpClient redirectHandler(Function<HttpClientResponse, Future<RequestOptions>> handler) {
		if(httpClient != null) {
			httpClient.redirectHandler(handler);
		}
		return this;
	}

	@Override
	public Function<HttpClientResponse, Future<RequestOptions>> redirectHandler() {
		if(httpClient != null) {
			return httpClient.redirectHandler();
		}
		return null;
	}

	@Override
	public boolean isMetricsEnabled() {
		return httpClient.isMetricsEnabled();
	}

	public class HalfOpenResult {

		private ResilientHttpClient r;

		HalfOpenResult(ResilientHttpClient resilientHttpClient) {
			this.r = resilientHttpClient;
		}

		public void fail() {
			r.openCircuit();
		}

		public void success() {
			r.closeCircuit();
		}
	}

	@Override
	public void updateSSLOptions(SSLOptions options, Handler<AsyncResult<Void>> handler) {
		if(httpClient != null) {
			httpClient.updateSSLOptions(options, handler);
		}
	}

	@Override
	public Future<Void> updateSSLOptions(SSLOptions options) {
		return httpClient.updateSSLOptions(options);
	}

	@Override
	public Future<Void> close() {
		return httpClient.close();
	}

	@Override
	public void close(Handler<AsyncResult<Void>> handler) {
		if(httpClient != null) {
			httpClient.close(handler);
		}
	}

	public HttpClient setHalfOpenHandler(Handler<HalfOpenResult> halfOpenHandler) {
		this.halfOpenHandler = halfOpenHandler;
		return this;
	}

	private HttpClientRequest preConfigureRequest(HttpClientRequest req) {
		req.exceptionHandler(throwable -> {
			log.error("SwiftHttpClient : request error", throwable);
			if (errorsCount.incrementAndGet() > threshold) {
				openCircuit();
			}
		});
		req.setTimeout(timeout);
		return req;
	}

	private void openCircuit() {
		log.info("SwiftHttpClient : open circuit");
		if (closedCircuit.getAndSet(false) && httpClient != null) {
			httpClient.close();
			httpClient = null;
		}
		errorsCount.set(0);
		vertx.setTimer(openDelay, new Handler<Long>() {
			@Override
			public void handle(Long aLong) {
				reconfigure();
			}
		});
	}

	private void reconfigure() {
		final int port = (uri.getPort() > 0) ? uri.getPort() : ("https".equals(uri.getScheme()) ? 443 : 80);
		HttpClientOptions options = new HttpClientOptions()
				.setDefaultHost(uri.getHost())
				.setDefaultPort(port)
				.setMaxPoolSize(16)
				.setSsl("https".equals(uri.getScheme()))
				.setKeepAlive(keepAlive)
				.setConnectTimeout(timeout);
		this.httpClient = vertx.createHttpClient(options);

//		this.httpClient.exceptionHandler(new Handler<Throwable>() {
//			@Override
//			public void handle(Throwable throwable) {
//				log.error("SwiftHttpClient : global error", throwable);
//				if (errorsCount.incrementAndGet() > threshold) {
//					openCircuit();
//				}
//			}
//		});
		log.info("SwiftHttpClient : half-close circuit");
		if (halfOpenHandler != null) {
			halfOpenHandler.handle(new HalfOpenResult(this));
		}
	}

	private void closeCircuit() {
		log.info("SwiftHttpClient : close circuit");
		closedCircuit.set(true);
	}

}
