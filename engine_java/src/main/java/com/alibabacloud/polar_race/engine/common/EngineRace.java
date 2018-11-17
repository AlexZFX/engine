package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongIntHashMap;
import io.netty.util.concurrent.FastThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class EngineRace extends AbstractEngine {

    private static Logger logger = LoggerFactory.getLogger(EngineRace.class);
    // key 长度 8B
//    private static final int KEY_LEN = 8;
//    // offset 长度 8B
//    private static final int OFF_LEN = 8;
    // key+offset 长度 16B
    private static final int KEY_AND_OFF_LEN = 12;
    // 线程数量
    private static final int THREAD_NUM = 64;
    // value 长度 4K
    private static final int VALUE_LEN = 4096;
    //    单个线程写入消息 100w
//    private static final int MSG_COUNT = 1000000;
    //    64个线程写消息 6400w
//    private static final int ALL_MSG_COUNT = 64000000;
    //每个map存储的key数量
    private static final int PER_MAP_COUNT = 1024000;

    private static final int SHIFT_NUM = 12;

    //    private static final int ALL_MSG_COUNT = 6400;
    //    每个文件存放 400w 个数据
//    private static final int MSG_COUNT_PERFILE = 4000000;
    //    存放 value 的文件数量 128
    private static final int FILE_COUNT = 64;

    private static final int HASH_VALUE = 0x3F;

    private static final int HASH_KEY = 0x3F;

    private static final LongIntHashMap[] keyMap = new LongIntHashMap[THREAD_NUM];

    static {
        for (int i = 0; i < THREAD_NUM; i++) {
            keyMap[i] = new LongIntHashMap(PER_MAP_COUNT, 0.98);
        }
    }

    //key 文件的fileChannel
    private static FileChannel[] keyFileChannels = new FileChannel[THREAD_NUM];

    private static AtomicInteger[] keyOffsets = new AtomicInteger[THREAD_NUM];

    //value 文件的fileChannel
    private static FileChannel[] fileChannels = new FileChannel[FILE_COUNT];

    private static AtomicInteger[] valueOffsets = new AtomicInteger[FILE_COUNT];

    private static FastThreadLocal<ByteBuffer> localKey = new FastThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() throws Exception {
            return ByteBuffer.allocateDirect(KEY_AND_OFF_LEN);
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
        RandomAccessFile randomAccessFile;
        // file是一个目录时进行接下来的操作
        if (file.isDirectory()) {
            try {
                //先构建keyFileChannel 和 初始化 map
                for (int i = 0; i < THREAD_NUM; i++) {
                    randomAccessFile = new RandomAccessFile(path + File.separator + i + ".key", "rw");
                    FileChannel channel = randomAccessFile.getChannel();
                    keyFileChannels[i] = channel;
                    keyOffsets[i] = new AtomicInteger((int) randomAccessFile.length());
                }
                CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
                for (int i = 0; i < THREAD_NUM; i++) {
                    if (!(keyOffsets[i].get() == 0)) {
                        final long off = keyOffsets[i].get();
                        final int finalI = i;
                        new Thread(() -> {
                            try {
                                int start = 0;
                                MappedByteBuffer mappedByteBuffer = keyFileChannels[finalI].map(FileChannel.MapMode.READ_ONLY, 0, off);
                                while (start < off) {
                                    start += KEY_AND_OFF_LEN;
                                    keyMap[finalI].put(mappedByteBuffer.getLong(), mappedByteBuffer.getInt());
                                }
                                unmap(mappedByteBuffer);
                                countDownLatch.countDown();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }).start();
                    } else {
                        countDownLatch.countDown();
                    }
                }
                countDownLatch.await();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
            //创建 FILE_COUNT个FileChannel 供write顺序写入
            for (int i = 0; i < FILE_COUNT; i++) {
                try {
                    randomAccessFile = new RandomAccessFile(path + File.separator + i + ".data", "rw");
                    FileChannel channel = randomAccessFile.getChannel();
                    fileChannels[i] = channel;
                    // 从 length处直接写入
                    valueOffsets[i] = new AtomicInteger((int) (randomAccessFile.length() >>> SHIFT_NUM));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            throw new EngineException(RetCodeEnum.IO_ERROR, "path不是一个目录");
        }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        long numkey = Util.bytes2long(key);
        int hash = valueFileHash(numkey);
        int off = valueOffsets[hash].getAndIncrement();
        try {
            //key写入文件
            localKey.get().putLong(0, numkey).putInt(8, off);
            localKey.get().position(0);
            keyFileChannels[hash].write(localKey.get(), keyOffsets[hash].getAndAdd(KEY_AND_OFF_LEN));
            //将value写入buffer
            localBufferValue.get().position(0);
            localBufferValue.get().put(value, 0, VALUE_LEN);
            //buffer写入文件
            localBufferValue.get().position(0);
            fileChannels[hash].write(localBufferValue.get(), ((long) off) << SHIFT_NUM);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "写入数据出错");
        }
    }


    @Override
    public byte[] read(byte[] key) throws EngineException {
        long numkey = Util.bytes2long(key);
        int hash = valueFileHash(numkey);
        long off = keyMap[hash].getOrDefault(numkey, -1);
        if (off == -1) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, numkey + "不存在");
        }
        try {
            localBufferValue.get().position(0);
            fileChannels[hash].read(localBufferValue.get(), off << SHIFT_NUM);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "读取数据出错");
        }
        localBufferValue.get().position(0);
        localBufferValue.get().get(localByteValue.get(), 0, VALUE_LEN);
        return localByteValue.get();
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
    }

    @Override
    public void close() {
        for (int i = 0; i < FILE_COUNT; i++) {
            try {
                keyFileChannels[i].close();
                fileChannels[i].close();
            } catch (IOException e) {
                logger.error("close error");
            }
        }
    }

    private static int valueFileHash(long key) {
        return (int) (key & HASH_VALUE);
    }

    private static int keyFileHash(long key) {
        return (int) (key & HASH_KEY);
    }

    private void unmap(MappedByteBuffer var0) {
        Cleaner var1 = ((DirectBuffer) var0).cleaner();
        if (var1 != null) {
            var1.clean();
        }
    }

}
