/*
 * Copyright (C) 2022 Glencoe Software, Inc. All rights reserved.
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

import java.util.Properties;

import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.upplication.s3fs.AmazonS3ClientFactory;

public class OmeroAmazonS3ClientFactory extends AmazonS3ClientFactory {

    private static final org.slf4j.Logger log =
            LoggerFactory.getLogger(OmeroAmazonS3ClientFactory.class);

    @Override
    protected AWSCredentialsProvider getCredentialsProvider(Properties props) {
        boolean anonymous = Boolean.parseBoolean(
                (String) props.get("s3fs_anonymous"));
        if (anonymous) {
            log.debug("Using anonymous credentials");
            return new AWSStaticCredentialsProvider(
                    new AnonymousAWSCredentials());
        } else {
            String profileName =
                    (String) props.get("s3fs_credential_profile_name");
            // Same instances and order from DefaultAWSCredentialsProviderChain
            return new AWSCredentialsProviderChain(
                    new ProfileCredentialsProvider(profileName),
                    new InstanceProfileCredentialsProvider(false)
            );
        }
    }

}
