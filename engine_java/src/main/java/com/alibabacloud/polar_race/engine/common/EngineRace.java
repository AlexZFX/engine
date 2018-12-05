package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongIntHashMap;
import io.netty.util.concurrent.FastThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class EngineRace extends AbstractEngine {

    private static Logger logger = LoggerFactory.getLogger(EngineRace.class);

    private static final int KEY_LEN = 8;
    //总key数量
    private static final int KEY_NUM = 64000000;
    //    private static final int KEY_NUM = 64000;
    // key+offset 长度 16B
    private static final int KEY_AND_OFF_LEN = 12;
    // 线程数量
    private static final int THREAD_NUM = 64;
    // value 长度 4K
    private static final int VALUE_LEN = 4096;

    private static final int SHIFT_NUM = 12;
    // 存放 value 的文件数量 128
    private static final int FILE_COUNT = 512;

    //128块 1块 8.4375m = 8640 KB = 8847360 B  1个文件 4320m
    private static final int VALUE_FILE_SIZE = 520 * 1024 * 1024;

    // 12m * 1024 * 1024 字节
    private static final int KEY_FILE_SIZE = 12582912;

//    private static final long BLOCK_SIZE = 8847360L;
    // BLOCK_SIZE / VALUE_LEN
//    private static final int MAX_NUM_PER_BLOCK = 2160;

    //    private static final int HASH_VALUE = 0x1FF;
    // 第i个key
//    private static final long[] keys = new long[KEY_NUM];
    private static long[] keys;
    // 第i个key的对应value的索引
//    private static final int[] offs = new int[KEY_NUM];
    private static int[] offs;

    private static LongIntHashMap[] map = new LongIntHashMap[THREAD_NUM];

    //key 文件的fileChannel
    private static FileChannel[] keyFileChannels = new FileChannel[THREAD_NUM];

    private static AtomicInteger[] keyOffsets = new AtomicInteger[THREAD_NUM];

    private static MappedByteBuffer[] keyMappedByteBuffers = new MappedByteBuffer[THREAD_NUM];

    //value 文件的fileChannel
    private static FileChannel[] fileChannels = new FileChannel[FILE_COUNT];
    //每个valueOffsets表示的都是第i个文件中的value数量
    private static AtomicInteger[] valueOffsets = new AtomicInteger[FILE_COUNT];

    // 一大块共享缓存
    private volatile ByteBuffer sharedBuffer;

    private static volatile ByteBuffer[] caches = new ByteBuffer[2];

    static {
        caches[0] = ByteBuffer.allocateDirect(VALUE_FILE_SIZE);
        caches[1] = ByteBuffer.allocateDirect(VALUE_FILE_SIZE);
    }

    private static final List<Thread> threadList = new ArrayList<>(THREAD_NUM);

    private boolean isFirst = true;
    //初始设置为 256 对应 符号位 100000000
    private volatile int fileReadCount = 256;

    private volatile int offReadCount = 255;

    private volatile boolean lock = true;

    private static ExecutorService executors = Executors.newSingleThreadExecutor();

    private final CyclicBarrier cyclicBarrier = new CyclicBarrier(THREAD_NUM, new Runnable() {
        @Override
        public void run() {
            ++fileReadCount;
            if (fileReadCount == FILE_COUNT) {
                fileReadCount = 0;
            }
            ++offReadCount;
            if (offReadCount == FILE_COUNT) {
                offReadCount = 0;
            }
            final int tempCount = fileReadCount;
//                try {
//                    caches[0].clear();
//                    fileChannels[tempCount].read(caches[0], 0);
//                    caches[0].flip();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
            if (isFirst) {
                sharedBuffer = caches[0];
                lock = true;
                executors.execute(() -> {
                    try {
                        caches[1].clear();
                        fileChannels[tempCount].read(caches[1], 0);
                        caches[1].flip();
                        lock = false;
//                        for (Thread thread : threadList) {
//                            LockSupport.unpark(thread);
//                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            } else {
                sharedBuffer = caches[1];
                executors.execute(() -> {
                    try {
                        caches[0].clear();
                        fileChannels[tempCount].read(caches[0], 0);
                        caches[0].flip();
//                        for (Thread thread : threadList) {
//                            LockSupport.unpark(thread);
//                        }
                        lock = false;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
            isFirst = !isFirst;
        }
    });


//    private static FastThreadLocal<ByteBuffer> localBufferKey = new FastThreadLocal<ByteBuffer>() {
//        @Override
//        protected ByteBuffer initialValue() throws Exception {
//            return ByteBuffer.allocate(KEY_AND_OFF_LEN);
//        }
//    };

    private static FastThreadLocal<ByteBuffer> localBufferValue = new FastThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() throws Exception {
            return ByteBuffer.allocateDirect(VALUE_LEN);
        }
    };

    private static FastThreadLocal<byte[]> localKeyBytes = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() throws Exception {
            return new byte[KEY_LEN];
        }
    };

    private static FastThreadLocal<byte[]> localValueBytes = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() throws Exception {
            return new byte[VALUE_LEN];
        }
    };

    private int CURRENT_KEY_NUM;


    @Override
    public void open(String path) throws EngineException {
        logger.info("--------open--------");
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
                // 初始化 file 文件
                for (int i = 0; i < FILE_COUNT; i++) {
                    randomAccessFile = new RandomAccessFile(path + File.separator + i + ".data", "rw");
                    FileChannel channel = randomAccessFile.getChannel();
                    fileChannels[i] = channel;
                    valueOffsets[i] = new AtomicInteger((int) (randomAccessFile.length() >>> SHIFT_NUM));
//                    logger.error("value file " + i + " off  = " + valueOffsets[i].get());
                }

                // 构建keyFileChannel 和 初始化 mmap
                for (int i = 0; i < THREAD_NUM; i++) {
                    randomAccessFile = new RandomAccessFile(path + File.separator + i + ".key", "rw");
                    FileChannel channel = randomAccessFile.getChannel();
                    keyFileChannels[i] = channel;
                    keyMappedByteBuffers[i] = channel.map(FileChannel.MapMode.READ_WRITE, 0, KEY_FILE_SIZE);
                    keyOffsets[i] = new AtomicInteger(0);
                    for (int j = 0; j < 8; j++) {
                        keyOffsets[i].getAndAdd(valueOffsets[(i << 3) + j].get() * KEY_AND_OFF_LEN);
                    }
//                    logger.error("key file " + i + " off  = " + keyOffsets[i].get());
                }

                CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
                CURRENT_KEY_NUM = 0;
                for (int i = 0; i < THREAD_NUM; i++) {
                    if (!(keyOffsets[i].get() == 0)) {
                        if (keys == null) {
                            keys = new long[KEY_NUM];
                            offs = new int[KEY_NUM];
                        }
                        final long off = keyOffsets[i].get();
//                        logger.info("第" + i + "个key文件的大小为 ：" + off + "B");
                        // 第i个文件写入 keys 的起始位置
                        final int temp = CURRENT_KEY_NUM;
                        CURRENT_KEY_NUM += off / KEY_AND_OFF_LEN;
                        final MappedByteBuffer buffer = keyMappedByteBuffers[i];
                        new Thread(() -> {
                            int start = 0;
                            int n = temp;
                            while (start < off) {
                                start += KEY_AND_OFF_LEN;
                                keys[n] = buffer.getLong();
                                offs[n++] = buffer.getInt();
                            }
                            countDownLatch.countDown();
                        }).start();
                    } else {
                        countDownLatch.countDown();
                    }
                }
                countDownLatch.await();

                if (keys == null) {
                    for (int i = 0; i < THREAD_NUM; i++) {
                        map[i] = new LongIntHashMap(1024000, 0.99);
                    }
                }

//                 对range时的第一块进行初始化
                fileChannels[fileReadCount].read(caches[0], 0);
                caches[0].flip();

                //获取完之后对key进行排序
                long sortStartTime = System.currentTimeMillis();
                heapSort(CURRENT_KEY_NUM);
                long sortEndTime = System.currentTimeMillis();
                logger.info("sort 耗时 " + (sortEndTime - sortStartTime) + "ms");
                logger.info("CURRENT_KEY_NUM = " + CURRENT_KEY_NUM);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            throw new EngineException(RetCodeEnum.IO_ERROR, "path不是一个目录");
        }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        long numkey = Util.bytes2long(key);
        int fileHash = valueFileHash(numkey);
        int keyHash = fileHash >>> 3;
        // value 写入的 offset，每个块内单独计算off
        int off;
        try {
            // 先将 value 写入 valueBuffer
            ByteBuffer buffer = localBufferValue.get();
            buffer.clear();
            buffer.put(value);
            buffer.flip();

            off = map[keyHash].getOrDefault(numkey, -1);

            if (off != -1) {
                // 如果已存在该key，则在key对应的原off位置写入value
                fileChannels[fileHash].write(buffer, off << SHIFT_NUM);
            } else { // 不存在该key时，先判断是否过块，过了则写入temp文件，修改off
                off = valueOffsets[fileHash].getAndIncrement();

                // 写入key + off
                map[keyHash].put(numkey, off);
                ByteBuffer keyBuffer = keyMappedByteBuffers[keyHash].slice();
                keyBuffer.position(keyOffsets[keyHash].getAndAdd(KEY_AND_OFF_LEN));
                keyBuffer.putLong(numkey).putInt(off);
                // 写入value
                fileChannels[fileHash].write(buffer, off << SHIFT_NUM);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new EngineException(RetCodeEnum.IO_ERROR, "写入数据出错");
        }

    }


    @Override
    public byte[] read(byte[] key) throws EngineException {
        long numkey = Util.bytes2long(key);
        int fileHash = valueFileHash(numkey);
        int off = getKey(numkey);
        if (off == -1) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, numkey + "不存在");
        }
        byte[] bytes = localValueBytes.get();
        try {
            ByteBuffer buffer = localBufferValue.get();
            //如果不在 块中，则去temp文件中读取
            fileChannels[fileHash].read(buffer, (long) off << SHIFT_NUM);
            buffer.flip();
            buffer.get(bytes, 0, VALUE_LEN);
            buffer.clear();
        } catch (Exception e) {
            e.printStackTrace();
            throw new EngineException(RetCodeEnum.IO_ERROR, "read 出错");
        }
        return bytes;
    }


    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        synchronized (threadList) {
            threadList.add(Thread.currentThread());
        }
        int num, count = 0;
        byte[] keyBytes = localKeyBytes.get();
        byte[] valueBytes = localValueBytes.get();
//        logger.info("in range CURRENT_KEY_NUM = " + CURRENT_KEY_NUM);
        try {
            // 第一次初始化sharedBuffer
            LockSupport.unpark(Thread.currentThread());
            for (int i = 0; i < FILE_COUNT; i++) {
                // 64 个屏障都到了才继续运行，运行前先获取buffer
                cyclicBarrier.await(20, TimeUnit.SECONDS);
                num = valueOffsets[offReadCount].get();
                ByteBuffer buffer = sharedBuffer.slice();
                for (int j = 0; j < num; ++j) {
                    buffer.position(offs[count] << SHIFT_NUM);
                    buffer.get(valueBytes);
                    long2bytes(keyBytes, keys[count++]);
                    visitor.visit(keyBytes, valueBytes);
                }
                while (lock) {
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new EngineException(RetCodeEnum.IO_ERROR, "range read io 出错");
        }
    }

    private void long2bytes(byte[] bytes, long key) {
        for (int i = 7; i >= 0; i--) {
            bytes[i] = (byte) (key & 0xFF);
            key >>= 8;
        }
    }


    @Override
    public void close() {
        try {
            logger.info("--------close--------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //    取前9位，分为512个文件
    private static int valueFileHash(long key) {
        return (int) (key >>> 55);
    }

    private int getKey(long numkey) {
        int l = 0, r = CURRENT_KEY_NUM - 1, mid;
        long num;
        while (l <= r) {
            mid = (l + r) >> 1;
            num = keys[mid];
            if (num < numkey) {
                l = mid + 1;
            } else if (num > numkey) {
                r = mid - 1;
            } else {
                return offs[mid];
            }
        }
        return -1;
    }

    /**
     * 对 index数组进行堆排
     *
     * @param startKeyNum 只排序前startKeyNum个数字
     */
    private void heapSort(int startKeyNum) {
        int end = startKeyNum - 1;
        for (int i = end >> 1; i >= 0; --i) {
            shiftDown(end, i);
        }
        for (int keyNum = end; keyNum > 0; --keyNum) {
            swap(keyNum, 0);
            shiftDown(keyNum - 1, 0);
        }
    }

    /**
     * index是从 0 开始的数组，则 k *2 之后要 + 1
     *
     * @param end 待排序的数组末尾
     * @param k   待shiftDown的位置
     */
    private void shiftDown(int end, int k) {
        int j = (k << 1) + 1;
        while (j <= end) {
            // 比较的数字是 index对应的key
            if (j + 1 <= end && (keys[j] < keys[j + 1])) {
                ++j;
            }
            if (keys[k] >= keys[j]) {
                break;
            }
            swap(k, j);
            k = j;
            j = (k << 1) + 1;
        }
    }

    private void swap(int i, int j) {
        long temp = keys[i];
        keys[i] = keys[j];
        keys[j] = temp;
        temp = offs[i];
        offs[i] = offs[j];
        offs[j] = (int) temp;
    }

}
