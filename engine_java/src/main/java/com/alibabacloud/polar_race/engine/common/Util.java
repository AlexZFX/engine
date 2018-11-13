package com.alibabacloud.polar_race.engine.common;

import java.nio.ByteBuffer;

/**
 * Created on 2018/11/13.
 */
public class Util {
    public static byte[] long2bytes(long key) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte)(key & 0xFF);
            key >>= 8;
        }
        return result;
    }

    // 更快的转换
    public static long bytes2long(byte[] bytes) {
        long result = 0;
        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result |= (bytes[i] & 0xFF);
        }
        return result;
    }

    public static byte[] genvalue(long key) {
        ByteBuffer buffer = ByteBuffer.allocate(4 * 1024);
        buffer.putLong(key);
        for (int i = 0; i < 4048 - 8; ++i) {
            buffer.put((byte) 0);
        }
        return buffer.array();
    }
}
