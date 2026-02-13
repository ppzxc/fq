package io.github.ppzxc.fq;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.Random;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class MVStoreFileQueueEnqueueBenchmark {

    @Param({"64", "128", "256", "512", "1024", "2048", "4096", "8192"})
    private int payloadSize;

    private MVStoreFileQueue<byte[]> queue;
    private Path tempDir;
    private byte[] payload;
    private Random random = new Random();

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("jmh-enqueue");
        String dbPath = tempDir.resolve("enqueue.db").toString();
        MVStoreFileQueueProperties properties = new MVStoreFileQueueProperties();
        properties.setQueueName("enqueue-bench");
        properties.setAutoCommitDisabled(true);
        properties.setBatchSize(1000);
        queue = new MVStoreFileQueue<>(properties, dbPath);
        payload = new byte[payloadSize];
        random.nextBytes(payload);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        queue.close();
        Files.walk(tempDir)
             .map(Path::toFile)
             .forEach(File::delete);
    }

    @Benchmark
    public void enqueue(Blackhole bh) {
        bh.consume(queue.enqueue(payload));
        // 속도 제한을 위한 미세 지연 (필요 시 조절)
        try {
            Thread.sleep(0, 100); 
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
