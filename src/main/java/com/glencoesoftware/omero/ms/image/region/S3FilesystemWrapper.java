/*
 * Copyright (C) 2021 Glencoe Software, Inc. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

package com.glencoesoftware.omero.ms.image.region;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.text.MessageFormat;

import org.slf4j.LoggerFactory;

public class S3FilesystemWrapper {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(S3FilesystemWrapper.class);

    /** FileSystem representing an S3 bucket */
    private FileSystem s3fs;

    /**
     * Default Constructor
     * @param accessKey AWS/Cloud access key
     * @param secretKey AWS/Cloud secret key
     * @param s3server S3 server the bucket is in, e.g. s3.eu-central-1.amazonaws.com
     */
    public S3FilesystemWrapper(
            String accessKey, String secretKey, String s3server) {
        String s3Server = "s3.amazonaws.com"; // example server name
        try {
            accessKey = URLEncoder.encode(accessKey, "UTF8");
            secretKey = URLEncoder.encode(secretKey, "UTF8");
        } catch (UnsupportedEncodingException e) {
            log.error("Failed to encode access key or secret key", e);
            return;
        }

        URI uri = URI.create(MessageFormat.format(
                "s3://{0}:{1}@{2}", accessKey, secretKey, s3Server));
        try {
            s3fs = FileSystems.newFileSystem(uri, null);
        } catch (IOException e) {
            log.error("Error creating S3 FileSystem", e);
        }
    }

    public FileSystem getS3fs() {
        return s3fs;
    }

}
