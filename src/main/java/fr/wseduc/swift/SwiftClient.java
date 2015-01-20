/*
 * Copyright © WebServices pour l'Éducation, 2014
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.wseduc.swift;

import fr.wseduc.swift.exception.AuthenticationException;
import fr.wseduc.swift.exception.StorageException;
import fr.wseduc.swift.storage.DefaultAsyncResult;
import fr.wseduc.swift.storage.StorageObject;
import fr.wseduc.swift.utils.*;
import org.vertx.java.core.*;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.*;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

public class SwiftClient {

	private static final Logger log = LoggerFactory.getLogger(SwiftClient.class);
	private final Vertx vertx;
	private final HttpClient httpClient;
	private final String defaultContainer;
	private String basePath;
	private String token;
	private long tokenLife;
	private long tokenPeriodic = 0l;

	public SwiftClient(Vertx vertx, URI uri) {
		this(vertx, uri, "documents");
	}

	public SwiftClient(Vertx vertx, URI uri, String container) {
		this(vertx, uri, container, 23 * 3600 * 1000);
	}

	public SwiftClient(Vertx vertx, URI uri, String container, long tokenLife) {
		this.vertx = vertx;
		this.httpClient = vertx.createHttpClient()
				.setHost(uri.getHost())
				.setPort(uri.getPort())
				.setMaxPoolSize(16)
				.setKeepAlive(false);
		this.defaultContainer = container;
		this.tokenLife = tokenLife;
	}

	public void authenticate(final String user, final String key, final AsyncResultHandler<Void> handler) {
		HttpClientRequest req = httpClient.get("/auth/v1.0", new Handler<HttpClientResponse>() {
			@Override
			public void handle(HttpClientResponse response) {
				if (response.statusCode() == 200) {
					token = response.headers().get("X-Storage-Token");
					try {
						basePath = new URI(response.headers().get("X-Storage-Url")).getPath();
						handler.handle(new DefaultAsyncResult<>((Void) null));
						if (tokenPeriodic != 0l) {
							vertx.cancelTimer(tokenPeriodic);
						}
						tokenPeriodic = vertx.setPeriodic(tokenLife, new Handler<Long>() {
							@Override
							public void handle(Long aLong) {
								authenticate(user, key, new AsyncResultHandler<Void>() {
									@Override
									public void handle(AsyncResult<Void> voidAsyncResult) {
										if (voidAsyncResult.failed()) {
											log.error("Periodic authentication error.", voidAsyncResult.cause());
										}
									}
								});
							}
						});
					} catch (URISyntaxException e) {
						handler.handle(new DefaultAsyncResult<Void>(new AuthenticationException(e.getMessage())));
					}
				} else {
					handler.handle(new DefaultAsyncResult<Void>(new AuthenticationException(response.statusMessage())));
				}
			}
		});
		req.putHeader("X-Auth-User", user);
		req.putHeader("X-Auth-Key", key);
		req.end();
	}

	public void uploadFile(HttpServerRequest request, Handler<JsonObject> handler) {
		uploadFile(request, defaultContainer, null, handler);
	}

	public void uploadFile(HttpServerRequest request, Long maxSize, Handler<JsonObject> handler) {
		uploadFile(request, defaultContainer, maxSize, handler);
	}

	public void uploadFile(final HttpServerRequest request, final String container, final Long maxSize,
			final Handler<JsonObject> handler) {
		request.expectMultiPart(true);
		request.uploadHandler(new Handler<HttpServerFileUpload>() {
			@Override
			public void handle(final HttpServerFileUpload upload) {
				upload.pause();
				final JsonObject metadata = FileUtils.metadata(upload);
				if (maxSize != null && maxSize < metadata.getLong("size", 0l)) {
					handler.handle(new JsonObject().putString("status", "error")
							.putString("message", "file.too.large"));
					return;
				}
				final String id = UUID.randomUUID().toString();
				final HttpClientRequest req = httpClient.put(basePath + "/" + container + "/" + id,
						new Handler<HttpClientResponse>() {
							@Override
							public void handle(HttpClientResponse response) {
								if (response.statusCode() == 201) {
									handler.handle(new JsonObject().putString("_id", id)
											.putString("status", "ok")
											.putObject("metadata", metadata));
								} else {
									handler.handle(new JsonObject().putString("status", "error"));
								}
							}
						});
				req.putHeader("X-Storage-Token", token);
				req.putHeader("Content-Type", metadata.getString("content-type"));
				req.putHeader("X-Object-Meta-Filename", metadata.getString("filename"));
				req.setChunked(true);
				upload.dataHandler(new Handler<Buffer>() {
					public void handle(Buffer data) {
						req.write(data);
					}
				});
				upload.endHandler(new VoidHandler() {
					public void handle() {
						req.end();
					}
				});
				upload.resume();
			}
		});
	}

	public void downloadFile(String id, final HttpServerRequest request) {
		downloadFile(id, request, defaultContainer, true, null, null, null);
	}

	public void downloadFile(String id, String container, final HttpServerRequest request) {
		downloadFile(id, request, container, true, null, null, null);
	}

	public void downloadFile(String id, final HttpServerRequest request,
		 boolean inline, String downloadName, JsonObject metadata, final String eTag) {
		downloadFile(id, request, defaultContainer, inline, downloadName, metadata, eTag);
	}

	public void downloadFile(String id, final HttpServerRequest request, String container,
			boolean inline, String downloadName, JsonObject metadata, final String eTag) {
		final HttpServerResponse resp = request.response();
		if (!inline) {
			java.lang.String name = FileUtils.getNameWithExtension(downloadName, metadata);
			resp.putHeader("Content-Disposition", "attachment; filename=\"" + name + "\"");
		}
		HttpClientRequest req = httpClient.get(basePath + "/" + container + "/" + id, new Handler<HttpClientResponse>() {
			@Override
			public void handle(HttpClientResponse response) {
				log.debug(response.statusCode() + " - " + response.statusMessage());
				response.pause();
				if (response.statusCode() == 200 || response.statusCode() == 304) {
					resp.putHeader("ETag", ((eTag != null) ? eTag : response.headers().get("ETag")));
				}
				if (response.statusCode() == 200) {
					resp.setChunked(true);
					response.dataHandler(new Handler<Buffer>() {
						@Override
						public void handle(Buffer event) {
							resp.write(event);
						}
					});
					response.endHandler(new Handler<Void>() {
						@Override
						public void handle(Void event) {
							resp.end();
						}
					});
					response.resume();
				} else {
					resp.setStatusCode(response.statusCode()).setStatusMessage(response.statusMessage()).end();
				}
			}
		});
		req.putHeader("X-Storage-Token", token);
		//req.putHeader("If-None-Match", request.headers().get("If-None-Match"));
		req.end();
		log.debug("Download file : " + id);
	}

	public void readFile(final String id, final AsyncResultHandler<StorageObject> handler) {
		readFile(id, defaultContainer, handler);
	}

	public void readFile(final String id, String container, final AsyncResultHandler<StorageObject> handler) {
		HttpClientRequest req = httpClient.get(basePath + "/" + container + "/" + id, new Handler<HttpClientResponse>() {
			@Override
			public void handle(final HttpClientResponse response) {
				response.pause();
				if (response.statusCode() == 200) {
					final Buffer buffer = new Buffer();
					response.dataHandler(new Handler<Buffer>() {
						@Override
						public void handle(Buffer event) {
							buffer.appendBuffer(event);
						}
					});
					response.endHandler(new Handler<Void>() {
						@Override
						public void handle(Void event) {
							StorageObject o = new StorageObject(
									id,
									buffer,
									response.headers().get("X-Object-Meta-Filename"),
									response.headers().get("Content-Type")
							);
							handler.handle(new DefaultAsyncResult<>(o));
						}
					});
					response.resume();
				} else {
					handler.handle(new DefaultAsyncResult<StorageObject>(new StorageException(response.statusMessage())));
				}
			}
		});
		req.putHeader("X-Storage-Token", token);
		req.end();
	}

	public void writeFile(StorageObject object, final AsyncResultHandler<String> handler) {
		writeFile(object, defaultContainer, handler);
	}

	public void writeFile(StorageObject object, String container, final AsyncResultHandler<String> handler) {
		final String id = (object.getId() != null) ? object.getId() : UUID.randomUUID().toString();
		final HttpClientRequest req = httpClient.put(basePath + "/" + container + "/" + id,
				new Handler<HttpClientResponse>() {
					@Override
					public void handle(HttpClientResponse response) {
						if (response.statusCode() == 201) {
							handler.handle(new DefaultAsyncResult<>(id));
						} else {
							handler.handle(new DefaultAsyncResult<String>(new StorageException(response.statusMessage())));
						}
					}
				});
		req.putHeader("X-Storage-Token", token);
		req.putHeader("Content-Type", object.getContentType());
		req.putHeader("X-Object-Meta-Filename", object.getFilename());
		req.end(object.getBuffer());
	}

	public void deleteFile(String id, final AsyncResultHandler<Void> handler) {
		deleteFile(id, defaultContainer, handler);
	}

	public void deleteFile(String id, String container, final AsyncResultHandler<Void> handler) {
		final HttpClientRequest req = httpClient.delete(basePath + "/" + container + "/" + id,
				new Handler<HttpClientResponse>() {
					@Override
					public void handle(HttpClientResponse response) {
						if (response.statusCode() == 204) {
							handler.handle(new DefaultAsyncResult<>((Void) null));
						} else {
							handler.handle(new DefaultAsyncResult<Void>(new StorageException(response.statusMessage())));
						}
					}
				});
		req.putHeader("X-Storage-Token", token);
		req.end();
	}

	public void copyFile(String from, final AsyncResultHandler<String> handler) {
		copyFile(from, defaultContainer, handler);
	}

	public void copyFile(String from, String container, final AsyncResultHandler<String> handler) {
		final String id = UUID.randomUUID().toString();
		final HttpClientRequest req = httpClient.put(basePath + "/" + container + "/" + id,
				new Handler<HttpClientResponse>() {
					@Override
					public void handle(HttpClientResponse response) {
						if (response.statusCode() == 201) {
							handler.handle(new DefaultAsyncResult<>(id));
						} else {
							handler.handle(new DefaultAsyncResult<String>(new StorageException(response.statusMessage())));
						}
					}
				});
		req.putHeader("X-Storage-Token", token);
		req.putHeader("Content-Length", "0");
		req.putHeader("X-Copy-From", "/" + container + "/" + from);
		req.end();
	}

	public void close() {
		if (tokenPeriodic != 0l) {
			vertx.cancelTimer(tokenPeriodic);
		}
		if (httpClient != null) {
			httpClient.close();
		}
	}

}
