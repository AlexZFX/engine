package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongIntHashMap;
import io.netty.util.concurrent.FastThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class EngineRace extends AbstractEngine {

    private static Logger logger = LoggerFactory.getLogger(EngineRace.class);

    private static final int KEY_LEN = 8;
    //总key数量
    private static final int KEY_NUM = 64000000;
    // key+offset 长度 12B
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

    private static long[] keys;
    // 第i个key的对应value的索引
    private static int[] offs;

    private static LongIntHashMap[] map = new LongIntHashMap[THREAD_NUM];

    private static AtomicInteger[] keyOffsets = new AtomicInteger[THREAD_NUM];

    private static MappedByteBuffer[] keyMappedByteBuffers = new MappedByteBuffer[THREAD_NUM];

    //value 文件的fileChannel
    private static FileChannel[] fileChannels = new FileChannel[FILE_COUNT];
    //每个valueOffsets表示的都是第i个文件中的value数量
    private static AtomicInteger[] valueOffsets = new AtomicInteger[FILE_COUNT];

    // 一大块共享缓存
    private volatile ByteBuffer sharedBuffer;

    private static volatile ByteBuffer[] caches;

    private boolean isFirst = true;
    //初始设置为 256 对应 符号位 100000000
    private volatile int fileReadCount = 0;

    private volatile int offReadCount = -1;

    private static ExecutorService executors = Executors.newSingleThreadExecutor();

    private static LinkedBlockingQueue<ByteBuffer> list = new LinkedBlockingQueue<>(1);

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
            try {
                sharedBuffer = list.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (isFirst) {
                executors.execute(() -> {
                    try {
                        caches[1].clear();
                        fileChannels[tempCount].read(caches[1], 0);
                        caches[1].flip();
                        list.offer(caches[1], 1, TimeUnit.SECONDS);
                    } catch (Exception e) {
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
                        list.put(caches[0]);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
            isFirst = !isFirst;
        }
    });

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

    private int CURRENT_KEY_NUM = 0;

    private int MID_KEY_NUM = 0;


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
                }

                // 构建keyFileChannel 和 初始化 mmap
                for (int i = 0; i < THREAD_NUM; i++) {
                    randomAccessFile = new RandomAccessFile(path + File.separator + i + ".key", "rw");
                    FileChannel channel = randomAccessFile.getChannel();
                    keyMappedByteBuffers[i] = channel.map(FileChannel.MapMode.READ_WRITE, 0, KEY_FILE_SIZE);
                    keyOffsets[i] = new AtomicInteger(0);
                    for (int j = 0; j < 8; j++) {
                        keyOffsets[i].getAndAdd(valueOffsets[(i << 3) + j].get() * KEY_AND_OFF_LEN);
                    }
                }

                for (int i = 0; i < 32; i++) {
                    MID_KEY_NUM += keyOffsets[i].get() / 12;
                }

                CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
                for (int i = 0; i < THREAD_NUM; i++) {
                    // 只要进入判断则说明是在读取过程中
                    if (!(keyOffsets[i].get() == 0)) {
                        if (caches == null) {
                            caches = new ByteBuffer[2];
                            caches[0] = ByteBuffer.allocateDirect(VALUE_FILE_SIZE);
                            caches[1] = ByteBuffer.allocateDirect(VALUE_FILE_SIZE);
                        }
                        if (keys == null) {
                            keys = new long[KEY_NUM];
                            offs = new int[KEY_NUM];
                        }
                        final int off = keyOffsets[i].get();
                        // 第i个文件写入 keys 的起始位置
                        final int start = CURRENT_KEY_NUM;
                        CURRENT_KEY_NUM += off / KEY_AND_OFF_LEN;
                        final int end = CURRENT_KEY_NUM;
                        final MappedByteBuffer buffer = keyMappedByteBuffers[i];
                        new Thread(() -> {
                            int n = start;
                            while (n < end) {
                                keys[n] = buffer.getLong();
                                offs[n++] = buffer.getInt();
                            }
                            quickSort(start, end - 1);
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
//                对range时的第一块进行初始化
                if (caches != null) {
                    executors.execute(() -> {
                        try {
                            fileChannels[0].read(caches[0], 0);
                            caches[0].flip();
                            list.put(caches[0]);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                }
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
        int num, count = 0;
        byte[] keyBytes = localKeyBytes.get();
        byte[] valueBytes = localValueBytes.get();
        try {
            // 第一次初始化sharedBuffer
            for (int i = 0; i < FILE_COUNT; i++) {
                // 64 个屏障都到了才继续运行，运行前先获取buffer
                cyclicBarrier.await(10, TimeUnit.SECONDS);
                num = valueOffsets[offReadCount].get();
                ByteBuffer buffer = sharedBuffer.slice();
                for (int j = 0; j < num; ++j) {
                    buffer.position(offs[count] << SHIFT_NUM);
                    buffer.get(valueBytes);
                    long2bytes(keyBytes, keys[count++]);
                    visitor.visit(keyBytes, valueBytes);
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
            executors.shutdownNow();
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
        if (numkey < 0) {
            return binary_search(numkey, MID_KEY_NUM, CURRENT_KEY_NUM - 1);
        } else {
            return binary_search(numkey, 0, MID_KEY_NUM - 1);
        }
    }

    private int binary_search(long numkey, int l, int r) {
        int mid;
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
     * 正序排序
     *
     * @param l
     * @param r
     */
    private void quickSort(int l, int r) {
        if (l >= r) {
            return;
        }
        int p = partition(l, r);
        quickSort(l, p - 1);
        quickSort(p + 1, r);
    }

    private int partition(int l, int r) {
        int low = l, high = r;
        while (low < high) {
            while (low < high && keys[low] < keys[high]) {
                --high;
            }
            if (low < high) {
                swap(low, high);
            }
            while (low < high && keys[high] > keys[low]) {
                ++low;
            }
            if (low < high) {
                swap(low, high);
                --high;
            }
        }
        return low;
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
