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

    private FileSystem s3fs;

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
