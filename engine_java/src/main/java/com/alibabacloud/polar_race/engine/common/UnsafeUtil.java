package com.alibabacloud.polar_race.engine.common;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;

/**
 * Author : Alex
 * Date : 2018/11/18 21:53
 * Description :
 */
public class UnsafeUtil {

    private static final Unsafe unsafe = getUnsafe();

    private static long ADDRESS_FIELD_OFFSET;

    private static long BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);

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

    static long getAddress(Object object) {
        return unsafe.getLong(object, ADDRESS_FIELD_OFFSET);
    }

    /**
     * @param address target address
     * @param value   byte value
     */
    static void putByte(long address, byte value) {
        unsafe.putByte(address, value);
    }

    static void putByte(byte[] data, int index, byte value) {
        unsafe.putByte(data, BYTE_ARRAY_BASE_OFFSET + index, value);
    }

    static byte getByte(long address) {
        return unsafe.getByte(address);
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
