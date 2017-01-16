package fr.wseduc.swift.utils;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.http.*;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
	public HttpClient exceptionHandler(Handler<Throwable> handler) {
		httpClient.exceptionHandler(handler);
		return this;
	}

	@Override
	public HttpClient setMaxPoolSize(int i) {
		httpClient.setMaxPoolSize(i);
		return this;
	}

	@Override
	public int getMaxPoolSize() {
		return httpClient.getMaxPoolSize();
	}

	@Override
	public HttpClient setMaxWaiterQueueSize(int i) {
		httpClient.setMaxWaiterQueueSize(i);
		return this;
	}

	@Override
	public int getMaxWaiterQueueSize() {
		return httpClient.getMaxWaiterQueueSize();
	}

	@Override
	public HttpClient setConnectionMaxOutstandingRequestCount(int i) {
		httpClient.setConnectionMaxOutstandingRequestCount(i);
		return this;
	}

	@Override
	public int getConnectionMaxOutstandingRequestCount() {
		return httpClient.getConnectionMaxOutstandingRequestCount();
	}

	@Override
	public HttpClient setKeepAlive(boolean b) {
		httpClient.setKeepAlive(b);
		return this;
	}

	@Override
	public boolean isKeepAlive() {
		return httpClient.isKeepAlive();
	}

	@Override
	public HttpClient setPipelining(boolean b) {
		httpClient.setPipelining(b);
		return this;
	}

	@Override
	public boolean isPipelining() {
		return httpClient.isPipelining();
	}

	@Override
	public HttpClient setPort(int i) {
		httpClient.setPort(i);
		return this;
	}

	@Override
	public int getPort() {
		return httpClient.getPort();
	}

	@Override
	public HttpClient setHost(String s) {
		httpClient.setHost(s);
		return this;
	}

	@Override
	public String getHost() {
		return httpClient.getHost();
	}

	@Override
	public HttpClient connectWebsocket(String s, Handler<WebSocket> handler) {
		httpClient.connectWebsocket(s, handler);
		return this;
	}

	@Override
	public HttpClient connectWebsocket(String s, WebSocketVersion webSocketVersion, Handler<WebSocket> handler) {
		httpClient.connectWebsocket(s, webSocketVersion, handler);
		return this;
	}

	@Override
	public HttpClient connectWebsocket(String s, WebSocketVersion webSocketVersion, MultiMap multiMap, Handler<WebSocket> handler) {
		httpClient.connectWebsocket(s, webSocketVersion, multiMap, handler);
		return this;
	}

	@Override
	public HttpClient connectWebsocket(String s, WebSocketVersion webSocketVersion, MultiMap multiMap, Set<String> set, Handler<WebSocket> handler) {
		httpClient.connectWebsocket(s, webSocketVersion, multiMap, set, handler);
		return this;
	}

	@Override
	public HttpClient getNow(String s, Handler<HttpClientResponse> handler) {
		if (httpClient == null) {
			handler.handle(new ErrorHttpClientResponse(500, ""));
			return this;
		}
		httpClient.getNow(s, handler);
		return this;
	}

	@Override
	public HttpClient getNow(String s, MultiMap multiMap, Handler<HttpClientResponse> handler) {
		if (httpClient == null) {
			handler.handle(new ErrorHttpClientResponse(500, ""));
			return this;
		}
		httpClient.getNow(s, multiMap, handler);
		return this;
	}

	@Override
	public HttpClientRequest options(String s, Handler<HttpClientResponse> handler) {
		if (httpClient == null) {
			handler.handle(new ErrorHttpClientResponse(500, ""));
			return null;
		}
		final HttpClientRequest req = httpClient.options(s, handler);
		preConfigureRequest(handler, req);
		return req;
	}

	@Override
	public HttpClientRequest get(String s, Handler<HttpClientResponse> handler) {
		if (httpClient == null) {
			handler.handle(new ErrorHttpClientResponse(500, ""));
			return null;
		}
		final HttpClientRequest req = httpClient.get(s, handler);
		preConfigureRequest(handler, req);
		return req;
	}

	@Override
	public HttpClientRequest head(String s, Handler<HttpClientResponse> handler) {
		if (httpClient == null) {
			handler.handle(new ErrorHttpClientResponse(500, ""));
			return null;
		}
		final HttpClientRequest req = httpClient.head(s, handler);
		preConfigureRequest(handler, req);
		return req;
	}

	@Override
	public HttpClientRequest post(String s, Handler<HttpClientResponse> handler) {
		if (httpClient == null) {
			handler.handle(new ErrorHttpClientResponse(500, ""));
			return null;
		}
		final HttpClientRequest req = httpClient.post(s, handler);
		preConfigureRequest(handler, req);
		return req;
	}

	@Override
	public HttpClientRequest put(String s, Handler<HttpClientResponse> handler) {
		if (httpClient == null) {
			handler.handle(new ErrorHttpClientResponse(500, ""));
			return null;
		}
		final HttpClientRequest req = httpClient.put(s, handler);
		preConfigureRequest(handler, req);
		return req;
	}

	@Override
	public HttpClientRequest delete(String s, Handler<HttpClientResponse> handler) {
		if (httpClient == null) {
			handler.handle(new ErrorHttpClientResponse(500, ""));
			return null;
		}
		final HttpClientRequest req = httpClient.delete(s, handler);
		preConfigureRequest(handler, req);
		return req;
	}

	@Override
	public HttpClientRequest trace(String s, Handler<HttpClientResponse> handler) {
		if (httpClient == null) {
			handler.handle(new ErrorHttpClientResponse(500, ""));
			return null;
		}
		final HttpClientRequest req = httpClient.trace(s, handler);
		preConfigureRequest(handler, req);
		return req;
	}

	@Override
	public HttpClientRequest connect(String s, Handler<HttpClientResponse> handler) {
		if (httpClient == null) {
			handler.handle(new ErrorHttpClientResponse(500, ""));
			return null;
		}
		final HttpClientRequest req = httpClient.connect(s, handler);
		preConfigureRequest(handler, req);
		return req;
	}

	@Override
	public HttpClientRequest patch(String s, Handler<HttpClientResponse> handler) {
		if (httpClient == null) {
			handler.handle(new ErrorHttpClientResponse(500, ""));
			return null;
		}
		final HttpClientRequest req = httpClient.patch(s, handler);
		preConfigureRequest(handler, req);
		return req;
	}

	@Override
	public HttpClientRequest request(String s, String s1, final Handler<HttpClientResponse> handler) {
		if (httpClient == null) {
			handler.handle(new ErrorHttpClientResponse(500, ""));
			return null;
		}
		final HttpClientRequest req = httpClient.request(s, s1, handler);
		preConfigureRequest(handler, req);
		return req;
	}

	@Override
	public void close() {
		httpClient.close();
	}

	@Override
	public HttpClient setVerifyHost(boolean b) {
		httpClient.setVerifyHost(b);
		return this;
	}

	@Override
	public boolean isVerifyHost() {
		return httpClient.isVerifyHost();
	}

	@Override
	public HttpClient setConnectTimeout(int i) {
		httpClient.setConnectTimeout(i);
		return this;
	}

	@Override
	public int getConnectTimeout() {
		return httpClient.getConnectTimeout();
	}

	@Override
	public HttpClient setTryUseCompression(boolean b) {
		httpClient.setTryUseCompression(b);
		return this;
	}

	@Override
	public boolean getTryUseCompression() {
		return httpClient.getTryUseCompression();
	}

	@Override
	public HttpClient setMaxWebSocketFrameSize(int i) {
		httpClient.setMaxWebSocketFrameSize(i);
		return this;
	}

	@Override
	public int getMaxWebSocketFrameSize() {
		return httpClient.getMaxWebSocketFrameSize();
	}

	@Override
	public HttpClient setSendBufferSize(int i) {
		httpClient.setSendBufferSize(i);
		return this;
	}

	@Override
	public HttpClient setReceiveBufferSize(int i) {
		httpClient.setReceiveBufferSize(i);
		return this;
	}

	@Override
	public HttpClient setReuseAddress(boolean b) {
		httpClient.setReuseAddress(b);
		return this;
	}

	@Override
	public HttpClient setTrafficClass(int i) {
		httpClient.setTrafficClass(i);
		return this;
	}

	@Override
	public int getSendBufferSize() {
		return httpClient.getSendBufferSize();
	}

	@Override
	public int getReceiveBufferSize() {
		return httpClient.getReceiveBufferSize();
	}

	@Override
	public boolean isReuseAddress() {
		return httpClient.isReuseAddress();
	}

	@Override
	public int getTrafficClass() {
		return httpClient.getTrafficClass();
	}

	@Override
	public HttpClient setTrustAll(boolean b) {
		httpClient.setTrustAll(b);
		return this;
	}

	@Override
	public boolean isTrustAll() {
		return httpClient.isTrustAll();
	}

	@Override
	public HttpClient setSSL(boolean b) {
		httpClient.setSSL(b);
		return this;
	}

	@Override
	public boolean isSSL() {
		return httpClient.isSSL();
	}

	@Override
	public HttpClient setSSLContext(SSLContext sslContext) {
		httpClient.setSSLContext(sslContext);
		return this;
	}

	@Override
	public HttpClient setKeyStorePath(String s) {
		httpClient.setKeyStorePath(s);
		return this;
	}

	@Override
	public String getKeyStorePath() {
		return httpClient.getKeyStorePath();
	}

	@Override
	public HttpClient setKeyStorePassword(String s) {
		httpClient.setKeyStorePassword(s);
		return this;
	}

	@Override
	public String getKeyStorePassword() {
		return httpClient.getKeyStorePassword();
	}

	@Override
	public HttpClient setTrustStorePath(String s) {
		httpClient.setTrustStorePath(s);
		return this;
	}

	@Override
	public String getTrustStorePath() {
		return httpClient.getTrustStorePath();
	}

	@Override
	public HttpClient setTrustStorePassword(String s) {
		httpClient.setTrustStorePassword(s);
		return this;
	}

	@Override
	public String getTrustStorePassword() {
		return httpClient.getTrustStorePassword();
	}

	@Override
	public HttpClient setTCPNoDelay(boolean b) {
		httpClient.setTCPNoDelay(b);
		return this;
	}

	@Override
	public HttpClient setTCPKeepAlive(boolean b) {
		httpClient.setTCPKeepAlive(b);
		return this;
	}

	@Override
	public HttpClient setSoLinger(int i) {
		httpClient.setSoLinger(i);
		return this;
	}

	@Override
	public HttpClient setUsePooledBuffers(boolean b) {
		httpClient.setUsePooledBuffers(b);
		return this;
	}

	@Override
	public boolean isTCPNoDelay() {
		return httpClient.isTCPNoDelay();
	}

	@Override
	public boolean isTCPKeepAlive() {
		return httpClient.isTCPKeepAlive();
	}

	@Override
	public int getSoLinger() {
		return httpClient.getSoLinger();
	}

	@Override
	public boolean isUsePooledBuffers() {
		return httpClient.isUsePooledBuffers();
	}

	public HttpClient setHalfOpenHandler(Handler<HalfOpenResult> halfOpenHandler) {
		this.halfOpenHandler = halfOpenHandler;
		return this;
	}

	private void preConfigureRequest(final Handler<HttpClientResponse> handler, HttpClientRequest req) {
		req.exceptionHandler(new Handler<Throwable>() {
			@Override
			public void handle(Throwable throwable) {
				log.error("SwiftHttpClient : request error", throwable);
				if (errorsCount.incrementAndGet() > threshold) {
					openCircuit();
				}
				handler.handle(new ErrorHttpClientResponse(500,
						((throwable != null && throwable.getMessage() != null) ? throwable.getMessage() : "")));
			}
		});
		req.setTimeout(timeout);
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
		this.httpClient = vertx.createHttpClient()
				.setHost(uri.getHost())
				.setPort(port)
				.setMaxPoolSize(16)
				.setSSL("https".equals(uri.getScheme()))
				.setKeepAlive(keepAlive)
				.setConnectTimeout(timeout);
		this.httpClient.exceptionHandler(new Handler<Throwable>() {
			@Override
			public void handle(Throwable throwable) {
				log.error("SwiftHttpClient : global error", throwable);
				if (errorsCount.incrementAndGet() > threshold) {
					openCircuit();
				}
			}
		});
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
