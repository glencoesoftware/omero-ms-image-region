/*
 * Copyright (C) 2018 Glencoe Software, Inc. All rights reserved.
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
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ome.api.IProjection;
import ome.conditions.ResourceError;
import ome.conditions.ValidationException;
import ome.io.nio.DimensionsOutOfBoundsException;
import ome.io.nio.PixelBuffer;
import ome.util.PixelData;
import ome.model.core.Pixels;
import ome.model.enums.PixelsType;

/**
 * Implements projection functionality for Pixels sets as declared in {@link
 * IProjection}.  Adapted from {@link ome.services.projection.ProjectionBean}.
 */
public class ProjectionService {

    /** The logger for this class. */
    private static Logger log =
            LoggerFactory.getLogger(ProjectionService.class);

    public byte[] projectStack(Pixels pixels, PixelBuffer pixelBuffer,
                               int algorithm, int timepoint,
                               int channelIndex, int stepping,
                               int start, int end)
    {
        ProjectionContext ctx = new ProjectionContext();
        ctx.pixels = pixels;
        zIntervalBoundsCheck(start, end, ctx.pixels.getSizeZ());
        outOfBoundsStepping(stepping);
        outOfBoundsCheck(channelIndex, "channel");
        outOfBoundsCheck(timepoint, "timepoint");
        Integer v = ctx.pixels.getSizeT();
        if (timepoint >= v) {
            throw new ValidationException("timepoint must be <" + v);
        }
        v = ctx.pixels.getSizeC();
        if (channelIndex >= v) {
            throw new ValidationException("channel index must be <" + v);
        }
        try {
            PixelsType pixelsType = pixels.getPixelsType();
            ctx.planeSizeInPixels =
                ctx.pixels.getSizeX() * ctx.pixels.getSizeY();
            int planeSize =
                ctx.planeSizeInPixels * (pixelsType.getBitSize() / 8);
            byte[] buf = new byte[planeSize];
            ctx.from = pixelBuffer.getStack(channelIndex, timepoint);
            ctx.to = new PixelData(pixelsType.getValue(), ByteBuffer.wrap(buf));

            switch (algorithm) {
                case IProjection.MAXIMUM_INTENSITY: {
                    projectStackMax(ctx, stepping, start, end, false);
                    break;
                }
                case IProjection.MEAN_INTENSITY: {
                    projectStackMean(ctx, stepping, start, end, false);
                    break;
                }
                case IProjection.SUM_INTENSITY: {
                    projectStackSum(ctx, stepping, start, end, false);
                    break;
                }
                default: {
                    throw new IllegalArgumentException(
                            "Unknown algorithm: " + algorithm);
                }
            }
            return buf;
        }
        catch (IOException e) {
            String error = String.format(
                    "I/O error retrieving stack C=%d T=%d: %s",
                    channelIndex, timepoint, e.getMessage());
            log.error(error, e);
            throw new ResourceError(error);
        } catch (DimensionsOutOfBoundsException e) {
            String error = String.format(
                    "C=%d or T=%d out of range for Pixels Id %d: %s",
                    channelIndex, timepoint, ctx.pixels.getId(),
                    e.getMessage());
            log.error(error, e);
            throw new ValidationException(error);
        } finally {
            try {
                pixelBuffer.close();
            } catch (IOException e) {
                log.error("Buffer did not close successfully.", e);
                throw new ResourceError(
                        e.getMessage() + " Please check server log.");
            }
            if (ctx.from != null) {
                ctx.from.dispose();
            }
        }
    }

    /**
     * Ensures that a particular dimension value is not out of range (ex. less
     * than zero).
     * @param value The value to check.
     * @param name The name of the value to be used for error reporting.
     * @throws ValidationException If <code>value</code> is out of range.
     */
    private void outOfBoundsCheck(Integer value, String name) {
        if (value != null && value < 0) {
            throw new ValidationException(name + ": " + value + " < 0");
        }
    }

    /**
     * Ensures that a particular dimension value is not out of range.
     * @param value The value to check.
     * @throws ValidationException If <code>value</code> is out of range.
     */
    private void outOfBoundsStepping(Integer value) {
        if (value != null && value <= 0) {
            throw new ValidationException("stepping: " + value + " <= 0");
        }
    }

    /**
     * Ensures that a particular dimension value is not out of range (ex. less
     * than zero).
     * @param start The lower bound of the interval.
     * @param end The upper bound of the interval.
     * @param name The name of the value to be used for error reporting.
     * @throws ValidationException If <code>value</code> is out of range.
     */
    private void zIntervalBoundsCheck(int start, int end, Integer maxZ) {
        if (start < 0 || end < 0)
            throw new ValidationException(
                    "Z interval value cannot be negative.");
        if (start >= maxZ || end >= maxZ)
            throw new ValidationException(
                    "Z interval value cannot be >= "+maxZ);
    }

    /**
     * Projects a stack based on the maximum intensity at each XY coordinate.
     * @param ctx The context of our projection.
     * @param stepping Stepping value to use while calculating the projection.
     * For example, <code>stepping=1</code> will use every optical section from
     * <code>start</code> to <code>end</code> where <code>stepping=2</code> will
     * use every other section from <code>start</code> to <code>end</code> to
     * perform the projection.
     * @param start Optical section to start projecting from.
     * @param end Optical section to finish projecting.
     * @param doMinMax Whether or not to calculate the minimum and maximum of
     * the projected pixel data.
     */
    private void projectStackMax(ProjectionContext ctx, int stepping,
                                 int start, int end, boolean doMinMax) {
        int currentPlaneStart;
        double projectedValue, stackValue;
        double minimum = ctx.minimum;
        double maximum = ctx.maximum;
        for (int i = 0; i < ctx.planeSizeInPixels; i++) {
            projectedValue = 0;
            for (int z = start; z <= end; z += stepping) {
                currentPlaneStart = ctx.planeSizeInPixels * z;
                stackValue = ctx.from.getPixelValue(currentPlaneStart + i);
                if (stackValue > projectedValue) {
                    projectedValue = stackValue;
                }
            }
            ctx.to.setPixelValue(i, projectedValue);
            if (doMinMax) {
                minimum = projectedValue < minimum? projectedValue : minimum;
                maximum = projectedValue > maximum? projectedValue : maximum;
            }
        }
        ctx.minimum = minimum;
        ctx.maximum = maximum;
    }

    /**
     * Projects a stack based on the mean intensity at each XY coordinate.
     * @param from The raw pixel data from the stack to project from.
     * @param ctx The context of our projection.
     * source Pixels set pixels type will be used.
     * @param stepping Stepping value to use while calculating the projection.
     * For example, <code>stepping=1</code> will use every optical section from
     * <code>start</code> to <code>end</code> where <code>stepping=2</code> will
     * use every other section from <code>start</code> to <code>end</code> to
     * perform the projection.
     * @param start Optical section to start projecting from.
     * @param end Optical section to finish projecting.
     * @param doMinMax Whether or not to calculate the minimum and maximum of
     * the projected pixel data.
     */
    private void projectStackMean(ProjectionContext ctx, int stepping,
                                  int start, int end, boolean doMinMax) {
        projectStackMeanOrSum(ctx, stepping, start, end, true, doMinMax);
    }

    /**
     * Projects a stack based on the sum intensity at each XY coordinate.
     * @param ctx The context of our projection.
     * @param pixelsType The destination Pixels type. If <code>null</code>, the
     * source Pixels set pixels type will be used.
     * @param stepping Stepping value to use while calculating the projection.
     * For example, <code>stepping=1</code> will use every optical section from
     * <code>start</code> to <code>end</code> where <code>stepping=2</code> will
     * use every other section from <code>start</code> to <code>end</code> to
     * perform the projection.
     * @param start Optical section to start projecting from.
     * @param end Optical section to finish projecting.
     * @param doMinMax Whether or not to calculate the minimum and maximum of
     * the projected pixel data.
     */
    private void projectStackSum(ProjectionContext ctx, int stepping,
                                 int start, int end, boolean doMinMax) {
        projectStackMeanOrSum(ctx, stepping, start, end, false, doMinMax);
    }

    /**
     * Projects a stack based on the sum intensity at each XY coordinate with
     * the option to also average the sum intensity.
     * @param ctx The context of our projection.
     * @param pixelsType The destination Pixels type. If <code>null</code>, the
     * source Pixels set pixels type will be used.
     * @param stepping Stepping value to use while calculating the projection.
     * For example, <code>stepping=1</code> will use every optical section from
     * <code>start</code> to <code>end</code> where <code>stepping=2</code> will
     * use every other section from <code>start</code> to <code>end</code> to
     * perform the projection.
     * @param start Optical section to start projecting from.
     * @param end Optical section to finish projecting.
     * @param mean Whether or not we're performing an average post sum
     * intensity projection.
     * @param doMinMax Whether or not to calculate the minimum and maximum of
     * the projected pixel data.
     */
    private void projectStackMeanOrSum(ProjectionContext ctx, int stepping,
                                       int start, int end,
                                       boolean mean, boolean doMinMax) {
        double planeMaximum = ctx.to.getMaximum();

        int currentPlaneStart;
        double projectedValue, stackValue;
        double minimum = ctx.minimum;
        double maximum = ctx.maximum;
        for (int i = 0; i < ctx.planeSizeInPixels; i++) {
            projectedValue = 0;
            int projectedPlaneCount = 0;
            for (int z = start; z < end; z += stepping) {
                currentPlaneStart = ctx.planeSizeInPixels * z;
                stackValue = ctx.from.getPixelValue(currentPlaneStart + i);
                projectedValue += stackValue;
                projectedPlaneCount++;
            }
            if (mean) {
                projectedValue = projectedValue / projectedPlaneCount;
            }
            if (projectedValue > planeMaximum) {
                projectedValue = planeMaximum;
            }
            ctx.to.setPixelValue(i, projectedValue);
            if (doMinMax) {
                minimum = projectedValue < minimum? projectedValue : minimum;
                maximum = projectedValue > maximum? projectedValue : maximum;
            }
        }
        ctx.minimum = minimum;
        ctx.maximum = maximum;
    }

    /**
     * Stores the context of a projection operation.  Adapted from
     * {@link ome.services.projection.ProjectionBean.ProjectionContext}.
     */
    private class ProjectionContext {

        /** The Pixels set we're currently working on. */
        public Pixels pixels;

        /** Count of the number of pixels per plane for <code>pixels</code>. */
        public int planeSizeInPixels;

        /** Current minimum for the projected pixel data. */
        public double minimum = Double.MAX_VALUE;

        /** Current maximum for the projected pixel data. */
        public double maximum = Double.MIN_VALUE;

        /** The raw pixel data from the stack to project from. */
        public PixelData from;

        /** The raw pixel data buffer to project into. */
        public PixelData to;
    }
}
