package com.alibabacloud.polar_race.engine.common;

/**
 * Created on 2018/12/2.
 */

public class TestVisitor extends AbstractVisitor {
    @Override
    public void visit(byte[] key, byte[] value) {
        if (Util.bytes2long(key) != Util.bytes2long(value)) {
            System.out.println("匹配错误 key = " + Util.bytes2long(key) + "-" + Util.bytes2long(value));
//            System.out.println(Arrays.toString(value));
        } else {
//            System.out.println(Util.bytes2long(key)+" OK");
        }
    }
}
