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

package fr.wseduc.swift.utils;

import org.vertx.java.core.http.HttpServerFileUpload;
import org.vertx.java.core.json.JsonObject;

import java.text.Normalizer;

public class FileUtils {

	public static String getNameWithExtension(String downloadName, JsonObject metadata) {
		String name = downloadName;
		if (metadata != null && metadata.getString("filename") != null) {
			String filename = metadata.getString("filename");
			int fIdx = filename.lastIndexOf('.');
			String fExt = null;
			if (fIdx >= 0) {
				fExt = filename.substring(fIdx);
			}
			int dIdx = downloadName.lastIndexOf('.');
			String dExt = null;
			if (dIdx >= 0) {
				dExt = downloadName.substring(dIdx);
			}
			if (fExt != null && !fExt.equals(dExt)) {
				name += fExt;
			}
		}
		return (name != null) ? Normalizer.normalize(name, Normalizer.Form.NFC) : name;
	}

	public static JsonObject metadata(HttpServerFileUpload upload) {
		JsonObject metadata = new JsonObject();
		metadata.putString("name", upload.name());
		metadata.putString("filename", upload.filename());
		metadata.putString("content-type", upload.contentType());
		metadata.putString("content-transfer-encoding", upload.contentTransferEncoding());
		metadata.putString("charset", upload.charset().name());
		metadata.putNumber("size", upload.size());
		return metadata;
	}

}
