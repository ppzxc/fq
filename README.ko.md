# fq — 파일 큐

[![Maven Central](https://img.shields.io/maven-central/v/io.github.ppzxc/fq.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.ppzxc/fq)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build](https://github.com/ppzxc/fq/actions/workflows/main.yml/badge.svg)](https://github.com/ppzxc/fq/actions/workflows/main.yml)

[English](README.md)

**fq**는 [H2 MVStore](https://h2database.com/html/mvstore.html)를 기반으로 하는 Java용 영속형 파일 기반 FIFO 큐입니다. JVM 재시작 후에도 데이터가 보존되며, 스레드 안전하고 고처리량 직렬화 객체 저장에 최적화되어 있습니다.

## 특징

- **영속성** — JVM 재시작 후에도 데이터 유지; head/tail 포인터가 영구 저장됨
- **스레드 안전** — 설정 가능한 재시도 + 지수 백오프를 갖춘 공정 모드 `ReentrantReadWriteLock`
- **고처리량** — 설정 가능한 배치 커밋, 선택적 압축 지원
- **크기 제한 선택** — 선택적 `maxSize` 상한값 설정 가능
- **자동 압축** — 파일이 설정된 임계값을 초과하면 디스크 공간 자동 회수
- **런타임 의존성 최소** — SLF4J API와 H2 MVStore만 필요

## 설치

### Gradle

```groovy
dependencies {
    implementation 'io.github.ppzxc:fq:0.0.12'
}
```

### Maven

```xml
<dependency>
    <groupId>io.github.ppzxc</groupId>
    <artifactId>fq</artifactId>
    <version>0.0.12</version>
</dependency>
```

## 빠른 시작

```java
import io.github.ppzxc.fq.FileQueue;
import io.github.ppzxc.fq.FileQueueFactory;

// 기본 설정으로 큐 생성 (파일: {user.dir}/sys/que/local)
FileQueue<String> queue = FileQueueFactory.createMVStoreFileQueue();

// 삽입
queue.enqueue("hello");
queue.enqueue("world");

// 꺼내기
String first = queue.dequeue();  // "hello"

// 배치 꺼내기
List<String> batch = queue.dequeue(10);

// 상태 확인
long size = queue.size();
boolean empty = queue.isEmpty();

// 사용 후 반드시 닫기
queue.close();
```

### 경로 및 속성 커스터마이징

```java
import io.github.ppzxc.fq.MVStoreFileQueueProperties;

MVStoreFileQueueProperties props = MVStoreFileQueueProperties.builder()
    .batchSize(500)
    .maxSize(100_000L)
    .useCompress(true)
    .maxRetry(5)
    .build();

// 기본 디렉터리 아래에 이름 지정 큐 생성
FileQueue<MyObject> queue = FileQueueFactory.createMVStoreFileQueue(props, "my-queue");

// 명시적 경로로 생성
FileQueue<MyObject> queue2 = FileQueueFactory.createMVStoreFileQueue("/data/queues/my-queue", props);
```

## 설정

모든 옵션은 `MVStoreFileQueueProperties`의 빌더를 통해 설정합니다:

| 속성 | 기본값 | 설명 |
|---|---|---|
| `queueName` | `"queue"` | 파일 내 MVStore 맵 이름 |
| `batchSize` | `1000` | N번 쓰기마다 디스크에 커밋 |
| `maxSize` | `Long.MAX_VALUE` | 최대 요소 수 (0 = 무제한) |
| `useCompress` | `true` | LZF 압축 활성화 |
| `autoCommitDisabled` | `false` | MVStore 자동 커밋 비활성화 |
| `autoCommitBufferSize` | `1024` | 자동 커밋 버퍼 크기 (KB) |
| `cacheSize` | `1` | 읽기 캐시 크기 (MB) |
| `autoCompactFillRate` | `90` | MVStore 압축 목표 채움률 (%) |
| `compactByFileSize` | `50` | 파일이 이 크기(MB)를 초과하면 `compactFile()` 실행 |
| `maxCompactTime` | `60000` | 압축 호출당 최대 소요 시간 (ms) |
| `fair` | `true` | `ReentrantReadWriteLock` 공정 모드 |
| `tryLockTimeout` | `1` | 락 획득 타임아웃 값 |
| `tryLockTimeunit` | `SECONDS` | 락 획득 타임아웃 단위 |
| `maxRetry` | `3` | 최대 락 재시도 횟수 |
| `retryDelay` | `100` | 재시도 간 기본 대기 시간 (ms) |
| `retryBackoffMs` | `100` | 지수 백오프 증가량 (ms) |

## API 레퍼런스

### `FileQueue<T extends Serializable>`

| 메서드 | 설명 |
|---|---|
| `boolean enqueue(T value)` | 요소 하나를 추가 |
| `boolean enqueue(List<T> value)` | 요소 목록을 원자적으로 추가 |
| `T dequeue()` | 맨 앞 요소를 꺼내 반환; 비어있으면 `null` |
| `List<T> dequeue(int size)` | 최대 `size`개의 요소를 꺼내 반환 |
| `boolean isEmpty()` | 큐가 비어있으면 `true` 반환 |
| `long size()` | 현재 요소 수 반환 |
| `String fileName()` | 백킹 파일의 절대 경로 반환 |
| `void metric(String name)` | 내부 지표 로깅 (head, tail, size, 카운트) |
| `void compactFile()` | 파일이 크기 임계값을 초과하면 압축 실행 |
| `void close()` | 보류 중인 쓰기를 플러시하고 스토어 닫기 (멱등성 보장) |

### `FileQueueFactory`

| 메서드 | 설명 |
|---|---|
| `createMVStoreFileQueue()` | 기본 속성, `{user.dir}/sys/que/local`에 파일 생성 |
| `createMVStoreFileQueue(MVStoreFileQueueProperties, String fileName)` | 커스텀 속성, `{user.dir}/sys/que/{fileName}`에 파일 생성 |
| `createMVStoreFileQueue(String path, MVStoreFileQueueProperties)` | 커스텀 속성, 명시적 파일 경로 지정 |

## 빌드

```bash
# 빌드 및 전체 테스트 실행
./gradlew build

# 테스트만 실행
./gradlew test

# 특정 테스트 클래스 실행
./gradlew test --tests MVStoreFileQueueUnitTest

# JaCoCo 커버리지 리포트 생성 (build/reports/jacoco/test/html/)
./gradlew jacocoTestReport

# JMH 벤치마크 실행 (build/results/jmh/results.json)
./gradlew jmh
```

## 라이선스

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) 라이선스 하에 배포됩니다.
