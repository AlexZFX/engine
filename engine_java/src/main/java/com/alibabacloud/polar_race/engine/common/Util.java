package com.alibabacloud.polar_race.engine.common;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

/**
 * Created on 2018/11/13.
 */
public class Util {

    public static final Unsafe unsafe = getUnsafe();

    private static long ADDRESS_FIELD_OFFSET;

    private static final long BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);

    static {
        final Field addressField;
        try {
            addressField = Buffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            ADDRESS_FIELD_OFFSET = unsafe.objectFieldOffset(addressField);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static byte[] long2bytes(long key) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (key & 0xFF);
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

    public static long getAddress(Object object) {
        return unsafe.getLong(object, ADDRESS_FIELD_OFFSET);
    }

    /**
     * @param address target address
     * @param value   byte value
     */
    public static void putByte(long address, byte value) {
        unsafe.putByte(address, value);
    }

    public static void putByte(byte[] data, int index, byte value) {
        unsafe.putByte(data, BYTE_ARRAY_BASE_OFFSET + index, value);
    }

    private static Unsafe getUnsafe() {
        Field f = null;
        try {
            f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        // 不会运行到这一步才对，下面的不可用
        return Unsafe.getUnsafe();
    }

}
