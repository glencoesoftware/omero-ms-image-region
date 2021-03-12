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

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TiledbUtilTest {

    @Test public void getSubarrayDomainStdTest() {
        long[] subArrayDomain = TiledbUtils.getSubarrayDomainFromString(
                "[0,0,0,100:150,200:250]");
        Assert.assertTrue(Arrays.equals(
                subArrayDomain, new long[] {0,0,0,0,0,0,100,149,200,249}));
    }

}
