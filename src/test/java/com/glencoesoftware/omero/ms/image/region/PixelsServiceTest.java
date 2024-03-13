/*
 * Copyright (C) 2023 Glencoe Software, Inc. All rights reserved.
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

import com.glencoesoftware.omero.ms.core.PixelsService;

import java.io.IOException;
import java.io.File;
import java.nio.file.Files;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import omero.ApiUsageException;
import omero.model.ExternalInfo;
import omero.model.ExternalInfoI;
import omero.model.IObject;
import omero.model.Image;
import omero.model.ImageI;
import omero.model.Mask;
import omero.model.MaskI;
import omero.util.IceMapper;

import static omero.rtypes.rdouble;
import static omero.rtypes.rlong;
import static omero.rtypes.rstring;

public class PixelsServiceTest {

  private PixelsService pixelsService;
  private String uuid = UUID.randomUUID().toString();
  private String imageUri = "/data/ngff/image.zarr";
  private String labelUri = imageUri + "/0/labels/" + uuid;
  private Image image;
  private Mask mask;
  private String ENTITY_TYPE = "com.glencoesoftware.ngff:multiscales";
  private int ENTITY_ID = 3;

  @Before
  public void setUp() throws IOException {
      File pixelsDir = Files.createTempDirectory("pixels").toFile();
      pixelsDir.deleteOnExit();
      File memoDir = Files.createTempDirectory("memoizer").toFile();
      memoDir.deleteOnExit();
      pixelsService = new PixelsService(
          pixelsDir.getAbsolutePath(), false, memoDir, 0L, null, null, null, null, 0, 0, 0);
      mask = new MaskI();
      image = new ImageI();
  }

  private void addExternalInfo(IObject object, Integer entityId, String entityType, String lsid, String uuid) {
        ExternalInfo externalInfo = new ExternalInfoI();
        externalInfo.setEntityId(rlong(entityId));
        externalInfo.setEntityType(rstring(entityType));
        if (lsid != null) {
            externalInfo.setLsid(rstring(lsid));
        }
        if (uuid != null) {
            externalInfo.setUuid(rstring(uuid));
        }
        object.getDetails().setExternalInfo(externalInfo);
  }

  @Test
  public void testDefault() throws ApiUsageException, IOException {
      addExternalInfo(mask, ENTITY_ID, ENTITY_TYPE, labelUri, uuid);
      Assert.assertEquals(pixelsService.getUri((ome.model.roi.Mask) new IceMapper().reverse(mask)), labelUri);

      addExternalInfo(image, ENTITY_ID, ENTITY_TYPE, imageUri, uuid);
      Assert.assertEquals(pixelsService.getUri((ome.model.core.Image) new IceMapper().reverse(image)), imageUri);
  }

  @Test
  public void testGetUriNoExternalInfo() throws ApiUsageException, IOException {
      Assert.assertNull(pixelsService.getUri((ome.model.roi.Mask) new IceMapper().reverse(mask)));
      Assert.assertNull(pixelsService.getUri((ome.model.core.Image) new IceMapper().reverse(image)));
  }

  @Test
  public void testGetUriNoUuid() throws ApiUsageException, IOException {
      addExternalInfo(mask, ENTITY_ID, ENTITY_TYPE, labelUri, null);
      Assert.assertEquals(pixelsService.getUri((ome.model.roi.Mask) new IceMapper().reverse(mask)), labelUri);

      addExternalInfo(image, ENTITY_ID, ENTITY_TYPE, imageUri, null);
      Assert.assertEquals(pixelsService.getUri((ome.model.core.Image) new IceMapper().reverse(image)), imageUri);
  }

  @Test
  public void testGetUriNoLsid() throws ApiUsageException, IOException {
      addExternalInfo(mask, ENTITY_ID, ENTITY_TYPE, null, uuid);
      Assert.assertNull(pixelsService.getUri((ome.model.roi.Mask) new IceMapper().reverse(mask)));

      addExternalInfo(image, ENTITY_ID, ENTITY_TYPE, null, uuid);
      Assert.assertNull(pixelsService.getUri((ome.model.core.Image) new IceMapper().reverse(image)));
  }

  @Test
  public void testGetUriWrongEntityType() throws ApiUsageException, IOException {
      addExternalInfo(mask, ENTITY_ID, "multiscales", labelUri, uuid);
      Assert.assertNull(pixelsService.getUri((ome.model.roi.Mask) new IceMapper().reverse(mask)));

      addExternalInfo(image, ENTITY_ID, "multiscales", imageUri, uuid);
      Assert.assertNull(pixelsService.getUri((ome.model.core.Image) new IceMapper().reverse(image)));
  }

  @Test
  public void testGetUriWrongEntityId() throws ApiUsageException, IOException {
    addExternalInfo(mask, 1, ENTITY_TYPE, labelUri, uuid);
    Assert.assertNull(pixelsService.getUri((ome.model.roi.Mask) new IceMapper().reverse(mask)));

    addExternalInfo(image, 1, ENTITY_TYPE, imageUri, uuid);
    Assert.assertNull(pixelsService.getUri((ome.model.core.Image) new IceMapper().reverse(image)));
  }
}
