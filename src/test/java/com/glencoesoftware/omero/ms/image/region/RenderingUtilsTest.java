package com.glencoesoftware.omero.ms.image.region;

import org.testng.Assert;
import org.testng.annotations.Test;

public class RenderingUtilsTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFlipNullImage() {
        int[] nullArray = null;
        RenderingUtils.flip(nullArray, 4, 4, true, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFlipZeroXImage() {
        int[] src = {1};
        RenderingUtils.flip(src, 0, 4, true, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testFlipZeroYImage() {
        int[] src = {1};
        RenderingUtils.flip(src, 4, 0, true, true);
    }

    private void testFlip(
            int[] src, int sizeX, int sizeY,
            boolean flipHorizontal, boolean flipVertical) {
        int[] flipped = RenderingUtils.flip(
                src, sizeX, sizeY, flipHorizontal, flipVertical);
        for (int n = 0; n < sizeX * sizeY; n++){
            int new_col;
            if (flipHorizontal) {
                int old_col = n % sizeX;
                new_col = sizeX - 1 - old_col;
            } else {
                new_col = n % sizeX;
            }
            int new_row;
            if (flipVertical) {
                int old_row = n / sizeX;
                new_row = sizeY - 1 - old_row;
            } else {
                new_row = n / sizeX;
            }
            Assert.assertEquals(flipped[new_row * sizeX + new_col], n);
        }
    }

    private void testAllFlips(int[] src, int sizeX, int sizeY) {
        boolean flipHorizontal = false;
        boolean flipVertical = true;
        testFlip(src, sizeX, sizeY, flipHorizontal, flipVertical);

        flipHorizontal = true;
        flipVertical = false;
        testFlip(src, sizeX, sizeY, flipHorizontal, flipVertical);

        flipHorizontal = true;
        flipVertical = true;
        testFlip(src, sizeX, sizeY, flipHorizontal, flipVertical);
    }

    @Test
    public void testFlipEvenSquare2() {
        int sizeX = 4;
        int sizeY = 4;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipOddSquare(){
        int sizeX = 5;
        int sizeY = 5;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipWideRectangle() {
        int sizeX = 7;
        int sizeY = 4;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipTallRectangle() {
        int sizeX = 4;
        int sizeY = 7;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipSingleWidthRectangle() {
        int sizeX = 7;
        int sizeY = 1;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipSingleHeightRectangle() {
        int sizeX = 1;
        int sizeY = 7;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllFlips(src, sizeX, sizeY);
    }

    @Test
    public void testFlipSingleEntry() {
        int sizeX = 1;
        int sizeY = 1;
        int[] src = new int[sizeX * sizeY];
        for (int n = 0; n < sizeX * sizeY; n++){
            src[n] = n;
        }
        testAllFlips(src, sizeX, sizeY);
    }
}
