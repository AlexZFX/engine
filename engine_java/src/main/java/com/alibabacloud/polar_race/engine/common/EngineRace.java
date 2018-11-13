package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongLongHashMap;
import io.netty.util.concurrent.FastThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public class EngineRace extends AbstractEngine {

    private static Logger logger = LoggerFactory.getLogger(EngineRace.class);
    // key 长度 8B
    private static final int KEY_LEN = 8;
    // offset 长度 8B
    private static final int OFF_LEN = 8;
    // value 长度 4K
    private static final int VALUE_LEN = 4096;
    //    单个线程写入消息 100w
    private static final int MSG_COUNT = 1000000;
    //    64个线程写消息 6400w
    private static final int ALL_MSG_COUNT = 64000000;
//    private static final int ALL_MSG_COUNT = 6400;
    //    每个文件存放 400w 个数据
    private static final int MSG_COUNT_PERFILE = 4000000;
    //    存放 value 的文件数量 128
    private static final int FILE_COUNT = 128;

    private static final int HASH_VALUE = 0x7F;

    private static FileChannel keyFileChannel;

    private static AtomicLong keyFileOffset;

    private static final LongLongHashMap keyMap = new LongLongHashMap(ALL_MSG_COUNT, 0.99f);

    private static FileChannel[] fileChannels = new FileChannel[FILE_COUNT];

    private static AtomicLong[] offsets = new AtomicLong[FILE_COUNT];

    //线程私有的buffer，用于byte数组转long
    private static FastThreadLocal<ByteBuffer> localKey = new FastThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() throws Exception {
            return ByteBuffer.allocateDirect(KEY_LEN);
        }
    };

    private static FastThreadLocal<ByteBuffer> localBufferValue = new FastThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() throws Exception {
            return ByteBuffer.allocateDirect(VALUE_LEN);
        }
    };

    private static FastThreadLocal<byte[]> localByteValue = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() throws Exception {
            return new byte[VALUE_LEN];
        }
    };


    @Override
    public void open(String path) throws EngineException {
        File file = new File(path);
        // 创建目录
        if (!file.exists()) {
            if (!file.mkdir()) {
                throw new EngineException(RetCodeEnum.IO_ERROR, "创建文件目录失败：" + path);
            } else {
                logger.info("创建文件目录成功：" + path);
            }
        }

        //创建 FILE_COUNT个FileChannel 顺序写入
        RandomAccessFile randomAccessFile;
        if (file.isDirectory()) {
            for (int i = 0; i < FILE_COUNT; i++) {
                try {
                    randomAccessFile = new RandomAccessFile(path + File.separator + i + ".data", "rw");

                    FileChannel channel = randomAccessFile.getChannel();
                    fileChannels[i] = channel;
                    // 从 length处直接写入
                    offsets[i] = new AtomicLong(randomAccessFile.length());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            throw new EngineException(RetCodeEnum.IO_ERROR, "path不是一个目录");
        }
        File keyFile = new File(path + File.separator + "key");
        if (!keyFile.exists()) {
            try {
                keyFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // 从 index 文件建立 hashmap
        try {
            randomAccessFile = new RandomAccessFile(keyFile, "rw");
            keyFileChannel = randomAccessFile.getChannel();

            ByteBuffer keyBuffer = ByteBuffer.allocate(KEY_LEN);
            ByteBuffer offBuffer = ByteBuffer.allocate(KEY_LEN);
            keyFileOffset = new AtomicLong(randomAccessFile.length());
            long temp = 0, maxOff = keyFileOffset.get();
            while (temp < maxOff) {
                keyBuffer.position(0);
                keyFileChannel.read(keyBuffer, temp);
                temp += KEY_LEN;
                offBuffer.position(0);
                keyFileChannel.read(offBuffer, temp);
                temp += KEY_LEN;
                keyBuffer.position(0);
                offBuffer.position(0);
                keyMap.put(keyBuffer.getLong(), offBuffer.getLong());
            }
//            System.out.println(keyMap.keys.length);
//            System.out.println(keyMap.values.length);

//            for (long k : keyMap.keys) {
//                if (k != 0) {
//                    System.out.println(k + ":" + keyMap.get(k));
//                }
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        //此时已经将key放到 localkey里面去了
        long numkey = Util.bytes2long(key);
        int hash = hash(numkey);
//        logger.warn("key = "+ Arrays.toString(key));
//        logger.warn("numkey = " + numkey);
//        logger.warn(" hash = "+hash);


        long off = offsets[hash].getAndAdd(VALUE_LEN);
//        System.out.println(numkey + " - " + (off + 1));
//        System.out.println(Util.bytes2long(key) + " - " + Util.bytes2long(value));
        keyMap.put(numkey, off + 1);
        try {
            //key写入文件
            localKey.get().putLong(0, numkey);
            localKey.get().position(0);
            keyFileChannel.write(localKey.get(), keyFileOffset.getAndAdd(KEY_LEN));
            //对应的offset写入文件
            localKey.get().putLong(0, off + 1);
            localKey.get().position(0);
            keyFileChannel.write(localKey.get(), keyFileOffset.getAndAdd(KEY_LEN));
            //将value写入buffer
            localBufferValue.get().position(0);
            localBufferValue.get().put(value, 0, VALUE_LEN);
            //buffer写入文件
            localBufferValue.get().position(0);
            fileChannels[hash].write(localBufferValue.get(), off);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "写入数据出错");
        }
    }


    @Override
    public byte[] read(byte[] key) throws EngineException {
        long numkey = Util.bytes2long(key);
        int hash = hash(numkey);
        logger.warn("key = " + Arrays.toString(key));
        logger.warn("numkey = " + numkey);
        logger.warn(" hash = " + hash);

//        System.out.println(numkey);
//        System.out.println(hash);

        // key 不存在会返回0，避免跟位置0混淆，off写加一，读减一
        long off = keyMap.get(numkey);
        if (off == 0) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, numkey + "不存在");
        }
//        System.out.println(off - 1);
        try {
            localBufferValue.get().position(0);
            fileChannels[hash].read(localBufferValue.get(), off - 1);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "读取数据出错");
        }
        localBufferValue.get().position(0);
        localBufferValue.get().get(localByteValue.get(), 0, VALUE_LEN);
//        logger.warn("value = " + Arrays.toString(localByteValue.get()));
        return localByteValue.get();
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
    }

    @Override
    public void close() {
        for (int i = 0; i < FILE_COUNT; i++) {
            try {
                fileChannels[i].close();
            } catch (IOException e) {
                logger.error("close error");
            }
        }
    }


    private static int hash(long key) {
        return (int) (key & HASH_VALUE);
    }
}
