# 📘 CẨM NANG & LỘ TRÌNH LÀM CHỦ APACHE KAFKA TOÀN DIỆN
### *Từ Kỹ Sư Cơ Bản Đến Kiến Trúc Sư Hệ Thống (Junior to Senior / Tech Lead)*

---

## 📑 MỤC LỤC
1. [Tổng Quan & Kiến Trúc Cốt Lõi (Kafka Core Architecture)](#1-tổng-quan--kiến-trúc-cốt-lõi)
2. [Lập Trình Producer & Consumer Với Spring Boot](#2-lập-trình-producer--consumer-với-spring-boot)
3. [Độ Tin Cậy & Các Mẫu Thiết Kế Doanh Nghiệp (Enterprise Patterns)](#3-độ-tin-cậy--các-mẫu-thiết-kế-doanh-nghiệp)
   - 3.1 Idempotent Producer & Idempotent Consumer
   - 3.2 Kafka Transactions & Exactly-Once Semantics (EOS)
   - 3.3 Transactional Outbox Pattern (Xử lý bài toán Dual-Write)
   - 3.4 Dead Letter Queue (DLQ / DLT Pattern) & Exponential Backoff
4. [Xử Lý Luồng Dữ Liệu Thời Gian Thực (Kafka Streams)](#4-xử-lý-luồng-dữ-liệu-thời-gian-thực-kafka-streams)
   - 4.1 KStream vs KTable vs GlobalKTable
   - 4.2 Các loại Cửa Sổ Thời Gian (Windowing)
   - 4.3 Bài toán thực chiến: Phát hiện gian lận (Fraud Detection)
5. [Quản Lý Hợp Đồng Dữ Liệu (Schema Registry & Apache Avro)](#5-quản-lý-hợp-đồng-dữ-liệu)
6. [Change Data Capture (CDC) & Tích Hợp Hệ Thống](#6-change-data-capture-cdc--tích-hợp-hệ-thống)
7. [Tinh Chỉnh Hiệu Năng & Vận Hành (Performance Tuning & Internals)](#7-tinh-chỉnh-hiệu-năng--vận-hành)
8. [Bảo Mật Trong Kafka (Security)](#8-bảo-mật-trong-kafka)
9. [Tổng Hợp 2 Dự Án Thực Hành Đã Xây Dựng](#9-tổng-hợp-2-dự-án-thực-hành-đã-xây-dựng)
10. [Top 15 Câu Hỏi Phỏng Vấn Kafka Kinh Điển (Kèm Đáp Án)](#10-top-15-câu-hỏi-phỏng-vấn-kafka-kinh-điển-kèm-đáp-án-senior)
11. [Các Sự Cố Production & Runbook Xử Lý](#11-các-sự-cố-production--runbook-xử-lý)
12. [High Availability, Disaster Recovery & Nâng Cấp](#12-high-availability-disaster-recovery--nâng-cấp)
13. [Kiểm Thử Kafka](#13-kiểm-thử-kafka)
14. [Lộ Trình Học Kafka 8 Tuần](#14-lộ-trình-học-kafka-8-tuần)
15. [Tài Liệu Chính Thức Nên Đọc](#15-tài-liệu-chính-thức-nên-đọc)

---

## 1. TỔNG QUAN & KIẾN TRÚC CỐT LÕI

### 1.1 Kafka là gì?
Apache Kafka là một **Distributed Event Streaming Platform** (nền tảng truyền phát luồng sự kiện phân tán), có khả năng lưu trữ bền vững, mở rộng ngang và chịu lỗi khi được cấu hình đúng. Thông lượng và độ trễ thực tế phụ thuộc vào phần cứng, kích thước record, số partition, replication factor, mức durability, batching, compression và tải của consumer; không nên mặc định mọi hệ thống đều đạt hàng triệu bản tin/giây hoặc độ trễ dưới 10 ms.

### 1.2 Bảng Thuật Ngữ Cốt Lõi
| Thuật ngữ | Khái niệm & Vai trò thực tế |
| :--- | :--- |
| **Broker** | Server Kafka chạy dịch vụ lưu trữ và phân phối message. Một cluster gồm nhiều Broker kết nối nhau. |
| **KRaft Mode** | Cơ chế quản lý metadata và bầu controller dựa trên quorum của Kafka, thay thế ZooKeeper và đơn giản hóa vận hành cluster. |
| **Topic** | Luồng dữ liệu logic có tên, được chia thành các partition. Topic không hoàn toàn tương đương table SQL hay queue truyền thống. |
| **Partition** | Đơn vị song song và thứ tự của topic. Mỗi partition là một append-only log gồm nhiều segment và index trên đĩa. Tăng partition giúp tăng song song nhưng làm tăng metadata, file handle, thời gian rebalance/restore và chi phí vận hành; Kafka không scale vô hạn. |
| **Offset** | Số nguyên tự tăng định danh duy nhất cho từng message trong một Partition cụ thể. Mang tính chất bất biến (immutable). |
| **Message Key** | Khóa dùng để chọn partition. Với cùng serializer, partitioner và số partition, cùng key thường vào cùng partition để giữ thứ tự theo entity. Khi tăng số partition hoặc đổi partitioner, key có thể bị ánh xạ sang partition khác, vì vậy phải đánh giá tác động tới lịch sử thứ tự. |
| **Consumer Group** | Nhóm gồm nhiều Consumer instances cùng chia sẻ việc đọc dữ liệu từ các partition của Topic để cân bằng tải. |
| **ISR (In-Sync Replicas)** | Danh sách các replica đang theo kịp leader trong ngưỡng cho phép. Durability phụ thuộc đồng thời vào `acks`, replication factor, `min.insync.replicas`, trạng thái ISR và chính sách bầu leader. |

### 1.3 Đường đi của một Record
1. Producer serialize key/value, chọn partition rồi gom record thành batch.
2. Leader của partition append batch vào log; các follower fetch để sao chép.
3. Producer nhận ACK theo cấu hình `acks` và trạng thái ISR.
4. Consumer `poll()` record theo offset, xử lý nghiệp vụ rồi commit offset.
5. Offset commit chỉ thể hiện vị trí đọc của consumer group, không chứng minh side effect ở DB/API đã thành công.

### 1.4 Leader, Replica, High Watermark và Log Segment
- Mỗi partition có một leader và các follower replica.
- Producer/consumer giao tiếp với leader; follower sao chép dữ liệu từ leader.
- High Watermark là ranh giới offset đã được sao chép an toàn theo trạng thái ISR và có thể hiển thị cho consumer thông thường; transaction còn có Last Stable Offset riêng cho `read_committed`.
- Record chỉ trở nên khả kiến theo các điều kiện replication/transaction tương ứng; không nên coi việc leader vừa ghi xong là đã an toàn tuyệt đối.
- Partition được chia thành nhiều log segment với các index. Retention và compaction làm việc trên segment, không xóa từng record ngay lập tức.

---

## 2. LẬP TRÌNH PRODUCER & CONSUMER VỚI SPRING BOOT

### 2.1 Cấu hình Serializer / Deserializer chuẩn
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
    consumer:
      group-id: my-business-group
      auto-offset-reset: earliest # Đọc từ đầu nếu chưa có committed offset
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.example.events"
```

> Cấu hình serializer/deserializer phải khớp với contract thực tế. Không nên copy `trusted.packages: "*"` vào production. Với Avro/Protobuf, hãy dùng serializer tương ứng và Schema Registry.

### 2.2 Quy Tắc Phân Chia Partition & Consumer Group
- **1 Partition chỉ được đọc bởi tối đa 1 Consumer trong cùng 1 Consumer Group** tại một thời điểm (để đảm bảo thứ tự).
- Nếu Topic có **3 Partitions**:
  - Nếu Group có **1 Consumer**: Consumer đó đọc cả 3 partitions.
  - Nếu Group có **3 Consumers**: Mỗi Consumer có thể đọc 1 partition. Đây là mức song song tối đa cho topic này, chưa chắc là cấu hình tối ưu về tài nguyên.
  - Nếu Group có **5 Consumers**: 2 Consumer sẽ rơi vào trạng thái nhàn rỗi (idle/standby).
- **Scale ngang:** Số consumer xử lý song song trong một group không vượt quá số partition được gán. Tuy nhiên, trước khi tăng partition phải xác định bottleneck và đánh giá tác động đến key mapping, ordering, rebalance, chi phí broker và state restore.

### 2.3 Offset, Poll Loop và Commit
- `auto.offset.reset=earliest/latest` chỉ được dùng khi group chưa có committed offset hoặc offset đã hết hạn; nó không làm consumer luôn đọc từ đầu.
- Với at-least-once, chỉ commit offset sau khi side effect nghiệp vụ đã thành công.
- Nếu xử lý batch, chỉ commit tới offset liên tục cao nhất đã hoàn tất; không commit vượt qua record còn đang xử lý.
- `KafkaConsumer` không thread-safe. Khi đẩy record sang thread pool, phải quản lý `pause/resume`, ordering và offset thủ công rất cẩn thận.
- `max.poll.records` giúp giới hạn lượng record cần xử lý trong một vòng; `max.poll.interval.ms` phải lớn hơn thời gian xử lý xấu nhất có kiểm soát, không dùng việc tăng timeout để che giấu code chậm.
- Khi rebalance, consumer có thể mất quyền sở hữu partition và gặp `CommitFailedException`; handler cần dừng side effect không còn hợp lệ hoặc dựa vào idempotency để xử lý lại an toàn.

### 2.4 Ma Trận Crash Cần Nắm
| Thời điểm crash | Kết quả có thể xảy ra | Cách thiết kế |
| :--- | :--- | :--- |
| Trước xử lý | Record được đọc lại | Bình thường với at-least-once |
| Sau xử lý, trước commit offset | Side effect có thể lặp | Idempotent consumer, unique constraint |
| Sau commit, trước xử lý xong | Có thể mất xử lý | Không commit sớm |
| Sau gửi output, trước commit transaction | Transaction abort hoặc retry | Kafka transaction + `read_committed` |

### 2.5 Thiết Kế Topic, Key và Partition
- Chọn key theo đơn vị cần ordering, ví dụ `orderId`, không chọn ngẫu nhiên chỉ để phân tải.
- Tránh key có cardinality thấp hoặc phân phối lệch gây **hot partition**.
- Ước lượng partition theo throughput mục tiêu, thời gian xử lý, số consumer tối đa và giới hạn vận hành; benchmark trước khi chốt.
- Quy định rõ topic owner, schema owner, retention, compaction, replication factor, `min.insync.replicas`, kích thước record tối đa và quyền ACL.
- Hạn chế large message; thường nên lưu object lớn ở object storage và chỉ gửi metadata/URL qua Kafka.
- Phân biệt rõ **event** (sự kiện đã xảy ra), **command** (yêu cầu thực hiện) và **state/changelog** để đặt retention và xử lý lỗi đúng mục đích.

---

## 3. ĐỘ TIN CẬY & CÁC MẪU THIẾT KẾ DOANH NGHIỆP

```
          3 CẤP ĐỘ CAM KẾT PHÂN PHÁI (DELIVERY SEMANTICS)
          
1. At-Most-Once   : Commit offset trước khi xử lý -> thường không phát lại, nhưng có thể mất xử lý.
2. At-Least-Once  : Xử lý xong mới commit offset -> có thể xử lý lại; durability vẫn phụ thuộc producer, broker và retention.
3. Exactly-Once   : Transaction giúp read-process-write nguyên tử trong phạm vi Kafka; side effect ngoài Kafka vẫn cần idempotency/outbox.
```

### 3.1 Idempotent Producer & Idempotent Consumer
- **Idempotent Producer (`enable.idempotence=true`):**
  - Khi Producer gửi message, Broker gán cho nó `ProducerId (PID)` và `Sequence Number`.
  - Nếu Broker đã lưu nhưng gặp timeout mạng khiến Producer gửi lại $\rightarrow$ Broker phát hiện Sequence Number bị trùng và tự động loại bỏ, không lưu 2 lần.
- **Idempotent Consumer (Deduplication Pattern):**
  - Consumer có thể nhận lại cùng event khi crash sau side effect nhưng trước khi commit offset.
  - **Giải pháp:** Dùng `eventId` có unique constraint trong bảng `processed_events`, đồng thời ghi marker và cập nhật nghiệp vụ trong **cùng transaction DB**.
  ```java
  @Transactional
  public void handle(OrderCreated event) {
      // tryInsert dùng INSERT ... ON CONFLICT DO NOTHING hoặc tương đương.
      boolean firstTime = processedEventRepository.tryInsert(event.getEventId());
      if (!firstTime) return;
      orderRepository.applyBusinessChange(event);
  }
  ```
  - Không dùng `existsById()` rồi mới `insert`: hai consumer có thể cùng thấy “chưa tồn tại” và cùng xử lý.
  - Nếu side effect là API/email/payment bên ngoài, truyền idempotency key cho hệ thống đích hoặc lưu state machine để retry an toàn.

### 3.2 Kafka Transactions & Exactly-Once Semantics (EOS)
- Sử dụng mô hình **Read-Process-Write** nguyên tử:
  - Consumer đọc từ `topic-A`.
  - Xử lý logic và ghi kết quả sang `topic-B`.
  - Commit offset của `topic-A` *bên trong cùng Transaction* với bước ghi sang `topic-B`.
- **Cấu hình then chốt:**
  - Producer: `transaction-id-prefix: tx-`
  - Downstream Consumer: `isolation.level = read_committed` (Broker sẽ giấu toàn bộ các message của transaction bị Abort/Rollback).
- **Giới hạn:** Kafka transaction không tự rollback database, REST API, email hoặc payment gateway. Không mô tả hệ thống là exactly-once end-to-end nếu vẫn có side effect ngoài Kafka chưa được bảo vệ.

### 3.3 Transactional Outbox Pattern (Giải Quyết Dual-Write)
- **Vấn đề "Dual-Write":** Khi tạo Order, nếu vừa ghi DB vừa gửi Kafka:
  - Ghi DB xong, gửi Kafka bị rớt mạng $\rightarrow$ Mất message.
  - Gửi Kafka trước, ghi DB bị rollback $\rightarrow$ Bắn message "ma".
- **Giải pháp Outbox Pattern:**
  1. Ghi `Order` và `OutboxEvent` (status `PENDING`) vào Database trong **CÙNG 1 TRANSACTION SQL**.
  2. Nếu dùng poller: worker claim các dòng `PENDING`, publish và cập nhật `SENT` sau ACK; cần xử lý stale lock và duplicate.
  3. Nếu dùng Debezium Outbox: connector đọc transaction log của bảng outbox; thường không cần đánh dấu `SENT`, nhưng cần chính sách cleanup/retention riêng.
  $\Rightarrow$ Outbox loại bỏ khoảng hở dual-write giữa DB và Kafka, nhưng poller vẫn có thể phát trùng nếu crash sau ACK và trước khi đánh dấu `SENT`; CDC cũng không loại bỏ yêu cầu idempotency downstream. Event phải có ID ổn định và consumer phải idempotent.

### 3.4 Dead Letter Queue (DLQ / DLT) & Exponential Backoff
- **Poison Pill:** Một message có định dạng hỏng hoặc vi phạm nghiệp vụ khiến Consumer quăng Exception liên tục và làm kẹt Partition.
- **Phân loại trước khi retry:**
  - Lỗi tạm thời: timeout, rate limit, downstream unavailable $\rightarrow$ retry với exponential backoff + jitter.
  - Lỗi vĩnh viễn: schema sai, validation fail, entity không hợp lệ $\rightarrow$ không retry vô ích; đưa vào DLT/quarantine.
  - Business rejection: ví dụ thẻ bị từ chối $\rightarrow$ phát business event/compensation, không coi mặc định là lỗi kỹ thuật.
- **Quy trình xử lý:**
  1. Retry tối đa $N$ lần, có backoff, jitter và giới hạn concurrency để tránh retry storm.
  2. Nếu vẫn thất bại $\rightarrow$ đẩy message sang topic `topic-name.DLT`.
  3. DLT record phải giữ original topic, partition, offset, key, timestamp, consumer group, exception class/message và correlation ID.
  4. Alert theo tỷ lệ DLT/SLO; không chỉ “lưu rồi quên”.
  5. Có công cụ **redrive/replay** sau khi sửa dữ liệu hoặc code, đồng thời giữ idempotency.

> Retry topic hoặc DLT có thể làm các event sau vượt qua event lỗi và phá ordering theo key. Nếu nghiệp vụ yêu cầu strict ordering, phải chọn chiến lược block partition, state machine hoặc quarantine theo key.

---

## 4. XỬ LÝ LUỒNG DỮ LIỆU THỜI GIAN THỰC (KAFKA STREAMS)

### 4.1 Khái Niệm Nền Tảng
- **KStream:** Luồng dữ liệu sự kiện vô hạn (Fact Stream - mỗi bản ghi là một sự kiện mới, ví dụ: Click, Giao dịch).
- **KTable:** Đại diện cho trạng thái hiện tại (Snapshot/State) của dữ liệu theo từng Key (tương tự như bảng DB có Primary Key, bản ghi mới sẽ UPDATE đè lên bản ghi cũ của Key đó).
- **GlobalKTable:** Toàn bộ dữ liệu của topic được nhân bản đầy đủ trên mọi instance của ứng dụng (dùng cho các bảng danh mục nhỏ để Join mà không cần repartition).

### 4.2 Các Loại Cửa Sổ Thời Gian (Windowing)
1. **Tumbling Window (Cửa sổ cố định không chồng lấn):**
   - Ví dụ: `[00:00 - 00:05]`, `[00:05 - 00:10]`.
   - Dùng để thống kê doanh thu định kỳ 5 phút một lần.
2. **Hopping Window (Cửa sổ cố định, chồng lấn):**
   - Cửa sổ kéo dài 5 phút nhưng dịch chuyển mỗi 1 phút một lần.
   - Dùng để tính giá trị trung bình động (Moving Average).
3. **Sliding Window (Cửa sổ theo timestamp của record):**
   - Window được tạo khi record đi vào hoặc rời khoảng thời gian trượt; khác hopping window có bước nhảy cố định.
   - Phù hợp cho các bài toán so sánh sự kiện xảy ra gần nhau theo event time.
4. **Session Window (Cửa sổ theo phiên hoạt động):**
   - Nhóm các hành động dựa trên thời gian không hoạt động (Inactivity Gap).
   - Ví dụ: Một phiên lướt web của User kết thúc khi họ không click gì trong 30 phút.

### 4.3 Event Time, Late Event và Grace Period
- Kafka Streams mặc định làm việc dựa trên timestamp của record. Timestamp extractor sai sẽ làm kết quả window sai.
- Record đến không đúng thứ tự là bình thường trong hệ thống phân tán.
- `ofSizeWithNoGrace()` đặt grace bằng 0; record đến muộn sau khi window đóng sẽ bị loại.
- Production nên xác định rõ độ trễ chấp nhận được, grace period, retention của state store và việc có phát intermediate result hay chỉ phát kết quả cuối.
- Nếu dùng `suppress()`, phải giới hạn buffer hợp lý để tránh tăng bộ nhớ không kiểm soát.

### 4.4 Ứng Dụng: Phát Hiện Gian Lận (Fraud Detection)
```java
// Lắng nghe order-events -> gom nhóm theo user -> cửa sổ 30 giây
orderStream
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeAndGrace(
        Duration.ofSeconds(30),
        Duration.ofSeconds(10)))
    .aggregate(...)
    .filter((user, stats) -> stats.getCount() >= 3 || stats.getTotal() >= 50_000_000)
    .to("fraud-alerts");
```

### 4.5 Kafka Streams Khi Chạy Production
- `application.id` đồng thời xác định consumer group và tên các internal topic; đổi tùy tiện có thể khiến ứng dụng đọc lại dữ liệu từ đầu.
- Stateful operator dùng local state store (thường RocksDB) và changelog topic để phục hồi.
- `groupBy`, join hoặc đổi key có thể tạo repartition topic và làm tăng network/storage.
- Cấu hình standby replica phù hợp để giảm thời gian restore sau khi instance chết.
- Khi đổi topology/state store, cần kế hoạch upgrade, reset hoặc migration; không deploy thay đổi không tương thích một cách mù quáng.

---

## 5. QUẢN LÝ HỢP ĐỒNG DỮ LIỆU (SCHEMA REGISTRY & APACHE AVRO)

### 5.1 Rủi Ro Khi Dùng JSON Không Có Schema Governance
- JSON không xấu và không phải doanh nghiệp nào cũng cấm. Vấn đề là JSON tự do không có contract validation và compatibility check.
- **Tốn băng thông hơn binary format trong nhiều trường hợp:** Từng message JSON lặp lại field name như `{"orderId": "...", "amount": ...}`; mức chênh lệch thực tế phải benchmark.
- **Dễ làm vỡ downstream:** Nếu Producer đổi tên trường `amount` thành `price`, các Consumer cũ có thể nhận `null`, deserialize lỗi hoặc xử lý sai âm thầm.

### 5.2 Lợi Ích Của Schema Registry + Apache Avro
- Avro/Protobuf mã hóa binary và thường nhỏ hơn JSON, nhưng tỷ lệ tiết kiệm phụ thuộc payload. Binary serialization khác với Kafka batch compression (`zstd`, `lz4`, `snappy`, `gzip`).
- **Schema Evolution (Tiến hóa hợp đồng):**
  - **Backward Compatibility:** Consumer dùng schema mới vẫn đọc được dữ liệu do Producer cũ tạo ra.
  - **Forward Compatibility:** Consumer dùng schema cũ vẫn đọc được dữ liệu do Producer mới tạo ra.
  - **Full Compatibility:** Đảm bảo tương thích cả hai chiều.

### 5.3 Quy Tắc Event Contract
- Event cần có `eventId`, `eventType`, `eventVersion`, `occurredAt`, `producer`, `correlationId`, `causationId` và payload nghiệp vụ.
- Không xóa/đổi nghĩa field đang được consumer sử dụng. Field mới phải có default hoặc optional đúng quy tắc của schema format.
- Kiểm tra compatibility trong CI/CD trước khi deploy producer.
- Chọn subject naming strategy có chủ đích; không mặc định mọi topic đều dùng chung một schema.
- Topic version (`*.v2`) chỉ nên dùng khi thay đổi breaking thực sự và có kế hoạch migrate/retire; không tạo topic mới cho mọi thay đổi nhỏ tương thích.
- Không chia sẻ thư viện DTO Java như cách duy nhất để quản lý contract vì nó làm các service bị coupled theo ngôn ngữ và release cycle. Schema Registry nên là nguồn contract liên dịch vụ.

---

## 6. CHANGE DATA CAPTURE (CDC) & TÍCH HỢP HỆ THỐNG

### 6.1 Kafka Connect là gì?
Framework phân tán giúp kết nối Kafka với các hệ thống khác (Databases, S3, Elasticsearch) bằng connector có sẵn, thường giảm đáng kể code tích hợp nhưng vẫn cần cấu hình, vận hành và đôi khi phải viết custom connector/transform:
- **Source Connector:** Kéo dữ liệu từ nguồn vào Kafka (e.g. Debezium, JDBC Source).
- **Sink Connector:** Đẩy dữ liệu từ Kafka ra đích (e.g. Elasticsearch Sink, Snowflake Sink).

### 6.2 Debezium CDC (Change Data Capture)
- Đọc trực tiếp **Transaction Log** ở tầng thấp nhất của DB (WAL của PostgreSQL hoặc Binlog của MySQL).
- Các thay đổi `INSERT`, `UPDATE`, `DELETE` trên bảng được cấu hình có thể được chuyển thành change event. Độ trễ phụ thuộc DB, connector, snapshot, network và tải; không mặc định luôn chỉ vài mili-giây.
- Ứng dụng: Tự động làm tươi Redis Cache, đồng bộ tìm kiếm sang Elasticsearch, hoặc chuyển dữ liệu về Data Warehouse.

### 6.3 Vận Hành Kafka Connect
- Hiểu quan hệ worker, connector và task; tăng `tasks.max` không đảm bảo source connector có thể chạy song song.
- Theo dõi connector/task state, restart count, source/sink record lag, error rate và DLQ.
- Bảo vệ các internal topic `config`, `offset`, `status` bằng replication factor phù hợp.
- Cấu hình `errors.tolerance`, retry và DLQ có chủ đích; `errors.tolerance=all` có thể che giấu mất dữ liệu nếu không alert.
- Single Message Transform (SMT) chỉ nên làm biến đổi nhẹ; logic nghiệp vụ phức tạp nên nằm ở stream processor/service.
- Với CDC, phải hiểu snapshot mode, schema history, database log retention, tombstone/delete và quyền truy cập DB.

---

## 7. TINH CHỈNH HIỆU NĂNG & VẬN HÀNH (PERFORMANCE TUNING & INTERNALS)

### 7.1 Tinh Chỉnh Producer (Tối Đa Hóa Throughput & Độ An Toàn)
- `acks=all`: Leader chờ toàn bộ ISR hiện tại xác nhận. Cần kết hợp replication factor và `min.insync.replicas`; không gọi đây là chống mất dữ liệu tuyệt đối.
- `enable.idempotence=true`: Chống duplicate do producer retry trong phiên producer; yêu cầu cấu hình tương thích như `acks=all`, retries và `max.in.flight.requests.per.connection <= 5`.
- `compression.type=zstd`/`lz4`/`snappy`: Giảm network và storage nhưng tiêu tốn CPU; benchmark bằng payload thật.
- `batch.size` và `linger.ms`: Batch lớn tăng throughput/compression ratio nhưng có thể tăng latency và memory.
- `delivery.timeout.ms`: Giới hạn tổng thời gian một record được retry trước khi báo fail; ứng dụng phải kiểm tra callback/future, không được bỏ qua lỗi async.
- Theo dõi `buffer.memory`, `max.block.ms`, request latency và record error rate để phát hiện backpressure.

### 7.2 Tinh Chỉnh Consumer & Cơ Chế Rebalance
- **Consumer Lag:** Chỉ số quan trọng nhất khi vận hành:
  $$\text{Consumer Lag} = \text{Log End Offset} - \text{Current Consumer Offset}$$
  Nếu lag tăng liên tục, kiểm tra consumer crash, rebalance, processing time, DB/API downstream, retry, GC, network và hot partition trước khi quyết định scale.
- `max.poll.interval.ms`: Khoảng thời gian tối đa giữa 2 lần gọi `poll()`. Nếu logic xử lý tốn nhiều thời gian hơn ngưỡng này, Kafka coi Consumer đã chết và kích hoạt Rebalance!
- **Cooperative Sticky Assignor:** Rebalance tăng dần, cố gắng chỉ thu hồi partition cần thiết. Nó giảm gián đoạn nhưng không loại bỏ mọi pause hoặc lỗi commit.
- **Static membership (`group.instance.id`):** Có thể giảm rebalance do restart ngắn, nhưng mỗi instance phải có ID duy nhất và cần timeout hợp lý.
- **Consumer rebalance protocol mới:** Ở Kafka hiện đại có thể đánh giá `group.protocol=consumer`; heartbeat/session timeout và assignor được điều khiển khác so với classic protocol.

### 7.3 Log Retention vs Log Compaction
- **Log Retention:** Xóa message theo thời gian (`log.retention.hours=168` - 7 ngày) hoặc theo kích thước (`log.retention.bytes`).
- **Log Compaction:** Kafka cố gắng giữ ít nhất bản ghi mới nhất cho mỗi key, nhưng việc dọn diễn ra bất đồng bộ theo segment/cleaner. Trong một khoảng thời gian vẫn có thể đọc thấy nhiều phiên bản của key.
- **Tombstone:** Record có key và value `null` biểu diễn delete trong compacted topic; tombstone chỉ bị dọn sau thời gian cấu hình.
- **Retention + Compaction:** Có thể bật đồng thời; record vẫn có thể bị xóa theo retention nên không được mặc định compacted topic là kho lưu trữ vĩnh viễn.

### 7.4 Metrics và Alert Bắt Buộc
| Nhóm | Chỉ số cần theo dõi |
| :--- | :--- |
| Broker | Offline partition, under-replicated partition, ISR shrink/expand, request latency/error, active controller/quorum health |
| Storage | Disk usage/growth, log directory failure, page cache pressure, remote storage error nếu có |
| Producer | Record error/retry, request latency, batch size, compression rate, buffer exhaustion |
| Consumer | Lag theo partition, lag time/freshness, processing latency, rebalance rate, commit failure, poll idle ratio |
| Connect/Streams | Task failure, DLQ rate, restore lag, state-store restore time, dropped late record |

Alert cần gắn với SLO, ví dụ “99% order event được xử lý dưới 30 giây”, không chỉ alert khi lag khác 0.

### 7.5 Capacity Planning
- Dự báo bytes/second trước và sau replication, retention window, headroom khi một broker chết và tốc độ rebuild replica.
- Giữ đủ disk headroom; disk đầy có thể làm partition offline và kéo theo mất availability.
- Benchmark với record size, key distribution, compression, replication và consumer logic thật.
- Đặt quota cho producer/consumer để một tenant không làm nghẽn toàn cluster.

---

## 8. BẢO MẬT TRONG KAFKA (SECURITY)

1. **Mã hóa đường truyền (Encryption in Transit):** Bật **SSL/TLS** giữa Client $\leftrightarrow$ Broker và giữa các Broker với nhau.
2. **Xác thực danh tính (Authentication):**
   - **mTLS (Mutual TLS):** Xác thực bằng chứng chỉ hai chiều.
   - **SASL/SCRAM:** Xác thực bằng Username/Password có hash muối an toàn.
   - **SASL/OAUTHBEARER:** Xác thực bằng Token JWT/OAuth2.
3. **Phân quyền truy cập (Authorization):**
   - Sử dụng **Kafka ACLs (Access Control Lists)**:
     - Cho phép Service Đơn hàng chỉ có quyền `WRITE` vào topic `order-events`.
     - Cho phép Service Thanh toán chỉ có quyền `READ` từ topic `order-events`.
4. **Nguyên tắc vận hành bảo mật:**
   - Mỗi service/connector có principal riêng và chỉ được cấp quyền tối thiểu.
   - Không đặt password/token trực tiếp trong Git; dùng secret manager và quy trình rotate credential/certificate.
   - Bảo vệ cả broker listener, inter-broker/controller listener, Schema Registry, Kafka Connect và monitoring endpoint.
   - Audit thay đổi ACL/config và cảnh báo truy cập bất thường.
   - Kafka chủ yếu bảo vệ dữ liệu khi truyền bằng TLS; nếu cần encryption at rest, dùng disk/cloud encryption hoặc mã hóa payload theo yêu cầu dữ liệu.
   - Không gửi secret, token, thông tin thẻ hoặc PII không cần thiết vào event; retention dài khiến phạm vi rò rỉ lớn hơn.

---

## 9. TỔNG HỢP 2 DỰ ÁN THỰC HÀNH ĐÃ XÂY DỰNG

### 📌 Dự án 1: `kafka-learning-project` (Hands-on từng khái niệm)
- **Trạng thái đường dẫn:** Tài liệu cũ trỏ tới Windows nhưng hiện không tìm thấy thư mục dự án này dưới `/home/ubuntu/Videos`. Hãy coi danh sách dưới đây là checklist lab cần hoàn thành hoặc cập nhật lại đường dẫn khi dự án được khôi phục.
- **6 Bài Lab:**
  - Lab 1: Gửi/nhận String message cơ bản (`BasicProducer`, `BasicConsumer`).
  - Lab 2: Serialize & Deserialize JSON POJO (`OrderEvent`).
  - Lab 3: Key-Based Partition Routing & bảo đảm thứ tự FIFO.
  - Lab 4: Consumer Groups & Cân bằng tải song song (`concurrency = 3`).
  - Lab 5: Dead Letter Queue (DLQ / DLT) với Retry Listener.
  - Lab 6: Kafka Transactions & Exactly-Once (`isolation.level = read_committed`).

### 📌 Dự án 2: `kafka-ecommerce-eventdriven` (Microservices Event-Driven & Saga Pattern v2.0)
- **Thư mục hiện tại:** `/home/ubuntu/Videos/kafka-ecommerce`
- **Mô hình kiến trúc:** **Maven Multi-Module Microservices** (7 modules độc lập).
- **Các Dịch vụ thành phần:**
  - `common-contracts`: Thư viện Data Contracts & Event DTOs bất biến dùng chung.
  - `order-service` (Port 8081): Quản lý vòng đời đơn hàng, Transactional Outbox Pattern; nếu service này giữ saga state và quyết định bước tiếp theo thì nó đóng vai trò orchestrator/coordinator.
  - `inventory-service` (Port 8082): Quản lý tồn kho thực tế, Idempotent Consumer, Saga Compensating Rollback (xả kho khi lỗi).
  - `payment-service` (Port 8083): Xử lý thanh toán, Idempotent Guard, kích hoạt lệnh bù trừ khi thẻ bị từ chối.
  - `fraud-detection-service` (Port 8084): Kafka Streams Engine độc lập (Sliding Window 30s) phát hiện gian lận và khóa đơn tự động.
  - `notification-service` (Port 8085): Tiếp nhận toàn bộ luồng sự kiện để mô phỏng gửi Email/SMS/Push cho khách.
  - `api-gateway` (Port 8080): Single Entry Point định tuyến toàn cụm và phục vụ Mission Control Web Dashboard.
- **Kỹ thuật thực chiến nâng cao:**
  - **Database-per-Service:** Mỗi service sở hữu database riêng biệt (`orderdb`, `inventorydb`, `paymentdb`), triệt tiêu phụ thuộc dữ liệu.
  - **Saga Pattern:** Cần gọi đúng mô hình. Nếu `order-service` điều phối command/state thì đây là orchestration hoặc hybrid; chỉ gọi choreography thuần khi không có coordinator trung tâm và mỗi service phản ứng với event độc lập.
  - **Compensating Transactions (Lệnh Bù Trừ):** Tự động phát sinh `InventoryReleaseCommand` để xả kho khi bước thanh toán thất bại.
  - **Publish/Subscribe Đa Consumer Group:** Cùng 1 topic `order-created-events` được tiêu thụ song song bởi Kho và Kafka Streams mà không nghẽn nhau.

---

## 10. TOP 15 CÂU HỎI PHỎNG VẤN KAFKA KINH ĐIỂN (KÈM ĐÁP ÁN SENIOR)

### Q1: Làm sao đảm bảo thứ tự của Message trong Kafka?
> **Trả lời:** Kafka chỉ đảm bảo thứ tự message trên **từng Partition riêng lẻ**, không đảm bảo trên toàn bộ Topic. Để các message của một đối tượng luôn đúng thứ tự (ví dụ: các giao dịch của cùng 1 User), ta phải truyền `Key` (ví dụ `userId`). Kafka sẽ hash key đó để đưa tất cả message vào cùng một partition.

### Q2: Chuyện gì xảy ra nếu số lượng Consumer nhiều hơn số lượng Partition?
> **Trả lời:** Các Consumer vượt quá sẽ rơi vào trạng thái nhàn rỗi (idle) và không nhận được message nào, vì một partition chỉ được gán cho tối đa một consumer trong cùng consumer group tại một thời điểm.

### Q3: Phân biệt `acks=0`, `acks=1` và `acks=all`?
> - `acks=0`: Producer gửi xong là coi như thành công, không chờ Broker phản hồi (Tốc độ nhanh nhất, rủi ro mất dữ liệu cao nhất).
> - `acks=1`: Producer chờ leader ghi local log rồi báo OK. Nếu leader chết trước khi follower sao chép thì có thể mất dữ liệu.
> - `acks=all` (hoặc `-1`): Chờ toàn bộ ISR hiện tại xác nhận. Đây là mức mạnh nhất, nhưng durability còn phụ thuộc replication factor, `min.insync.replicas`, ISR và leader election. Kafka hiện đại mặc định `acks=all` khi không có cấu hình xung đột.

### Q4: Dual-Write Problem là gì và giải quyết thế nào với Kafka?
> **Trả lời:** Là tình huống một service cần vừa lưu dữ liệu vào Database, vừa gửi event lên Kafka. Vì 2 hệ thống này không chung transaction, lỗi mạng có thể làm một bên thành công còn bên kia thất bại (lệch dữ liệu). Giải pháp chuẩn ngành là dùng **Transactional Outbox Pattern**: lưu event vào bảng `outbox` trong cùng transaction SQL với dữ liệu chính, sau đó dùng Poller hoặc Debezium CDC đẩy lên Kafka.

### Q5: Consumer Lag là gì? Nếu Lag tăng vọt thì xử lý thế nào?
> **Trả lời:** Lag là độ chênh lệch giữa offset mới nhất trong topic và offset mà consumer đang đọc tới. Khi Lag tăng vọt:
> 1. Kiểm tra Consumer có bị chết hoặc bị dính lỗi timeout/rebalance không.
> 2. Kiểm tra lag có tập trung ở một partition/hot key hay không và đo DB/API/GC/network.
> 3. Tối ưu batch hoặc scale consumer trong giới hạn số partition. Chỉ tăng partition sau khi đánh giá key mapping và ordering.
> 4. Nếu dùng thread pool, phải quản lý ordering, `pause/resume` và commit offset để không commit vượt record chưa xử lý.

### Q6: Làm sao tránh được việc Consumer nhận message trùng lặp?
> **Trả lời:** Trong môi trường phân tán, Kafka hướng tới cam kết *At-Least-Once Delivery*. Để tránh trùng lặp ở tầng ứng dụng, Consumer phải thiết kế theo mẫu **Idempotent Consumer**: Lưu `eventId` hoặc `idempotencyKey` vào Database/Redis (Unique Constraint). Khi nhận message, kiểm tra nếu key đã tồn tại thì bỏ qua ngay.

### Q7: Tại sao Kafka lại nhanh vượt trội so với các Message Broker truyền thống?
> **Trả lời:** Nhờ 4 yếu tố thiết kế:
> 1. **Sequential I/O:** Ghi file append-only theo tuần tự vào đĩa cứng (tốc độ gần tương đương RAM).
> 2. **Zero-Copy Optimization:** Sử dụng system call `sendfile()`, chuyển dữ liệu trực tiếp từ OS Page Cache ra Network Card mà không cần copy qua user-space application memory.
> 3. **Page Cache tận dụng RAM của OS:** Dùng bộ nhớ đệm trang của Linux thay vì giữ object nặng trong JVM Heap (tránh Garbage Collection pause).
> 4. **Batching & Compression:** Gom nhiều tin nhắn thành lô lớn và nén trước khi gửi qua socket mạng.

### Q8: KTable khác KStream như thế nào?
> **Trả lời:**
> - `KStream` coi mỗi record là một bản cập nhật mới (Fact/Log) - Insert-only.
> - `KTable` coi mỗi record là một thao tác Upsert (Update nếu đã có key, Insert nếu chưa) - đại diện cho trạng thái snapshot mới nhất của dữ liệu.

### Q9: Dead Letter Topic (DLT) dùng để làm gì?
> **Trả lời:** Dùng để cô lập các tin nhắn độc (Poison Pills) không thể xử lý sau khi đã thử lại (retry) nhiều lần. Việc đưa vào DLT giúp giải phóng partition để Consumer tiếp tục xử lý các tin nhắn hợp lệ tiếp theo, đồng thời lưu trữ tin lỗi để kỹ sư kiểm tra và sửa đổi.

### Q10: KRaft mode thay thế ZooKeeper mang lại lợi ích gì?
> **Trả lời:**
> 1. Không cần duy trì cụm ZooKeeper riêng biệt $\rightarrow$ Tiết kiệm tài nguyên và đơn giản hóa vận hành.
> 2. Khởi động và khôi phục cluster nhanh hơn gấp nhiều lần.
> 3. Cải thiện khả năng mở rộng và thời gian phục hồi metadata so với kiến trúc ZooKeeper. Không nên cam kết “hàng triệu partition” cho mọi cluster; giới hạn thực tế phải được capacity test theo phiên bản và hạ tầng.

### Q11: Phân tán Giao Dịch trong Microservices bằng Saga Pattern thế nào?
> **Trả lời:** Trong Microservices, mỗi service có Database riêng nên không thể dùng `@Transactional` ACID truyền thống. Saga chia giao dịch lớn thành chuỗi các giao dịch cục bộ (Local Transactions). Khi một bước thất bại (ví dụ: thanh toán bị từ chối), hệ thống sẽ phát sinh **Compensating Transaction (Giao dịch bù trừ)** ngược lại các bước trước (ví dụ: xả kho đã giữ, hoàn điểm thưởng) để đảm bảo tính nhất quán cuối cùng (Eventual Consistency).

### Q12: So sánh Choreography Saga và Orchestration Saga?
> **Trả lời:**
> - **Choreography (Hướng sự kiện - Không có nhạc trưởng):** Các service tự lắng nghe event của nhau qua Kafka và tự quyết định hành động tiếp theo. Ưu điểm: Loose-coupling, hiệu năng cao, linh hoạt. Nhược điểm: Khó vẽ luồng tổng thể khi hệ thống có hàng chục bước.
> - **Orchestration (Có nhạc trưởng điều phối):** Dùng orchestrator như Camunda, Temporal hoặc coordinator service để giữ state và quyết định bước tiếp theo. Ưu điểm: Dễ theo dõi timeout/compensation. Nhược điểm: Coupling vào workflow và cần scale/HA đúng cách; orchestrator không mặc nhiên là single point of failure nếu triển khai replicated với durable state.

### Q13: Tại sao không nên chia sẻ chung Database giữa các Microservices?
> **Trả lời:** Chia sẻ chung Database vi phạm nguyên tắc bao đóng (loose coupling) của Microservices:
> 1. Khi 1 service đổi schema bảng, nó làm vỡ logic của tất cả các service khác đang đọc bảng đó.
> 2. Không thể chọn công nghệ lưu trữ phù hợp (ví dụ: Order dùng SQL, Analytics dùng ClickHouse/NoSQL).
> 3. Xung đột tài nguyên và khó phân quyền bảo mật độc lập.

### Q14: Khi scale out Outbox Publisher trên nhiều instance của Order Service thì giải quyết tranh chấp thế nào?
> **Trả lời:** Nếu chạy 3 pods của Order Service cùng quét bảng `outbox_events`:
> - Giải pháp 1: Dùng SQL `SELECT ... FOR UPDATE SKIP LOCKED` (chỉ khóa dòng chưa bị pod khác xử lý).
> - Giải pháp 2: Dùng **Debezium CDC** đọc Transaction Log của DB thay vì Poller. Kafka Connect đảm nhiệm HA connector/task; vẫn phải vận hành snapshot, offset, schema history, DB log retention và duplicate downstream.
> - Giải pháp 3: Dùng ShedLock hoặc Distributed Lock (Redis / Zookeeper) để chỉ định duy nhất 1 Pod làm Leader quét Outbox.

### Q15: Làm sao kiểm soát Event Versioning khi một Service cập nhật Event Payload?
> **Trả lời:**
> 1. Ưu tiên thay đổi tương thích trong cùng topic: không xóa/đổi nghĩa field cũ, field mới có default/optional đúng format.
> 2. Dùng **Schema Registry + Avro/Protobuf/JSON Schema** và compatibility check trong CI.
> 3. Chỉ tạo topic `.v2` cho breaking change thật sự, chạy song song trong thời gian migrate và có kế hoạch ngừng `.v1`.

---

## 11. CÁC SỰ CỐ PRODUCTION & RUNBOOK XỬ LÝ

### 11.1 Bảng Chẩn Đoán Nhanh
| Hiện tượng | Nguyên nhân thường gặp | Kiểm tra đầu tiên | Hướng xử lý |
| :--- | :--- | :--- | :--- |
| Consumer lag tăng đều | Consumer chậm, DB/API nghẽn, thiếu parallelism | Lag theo partition, processing latency, DB/API latency | Tối ưu bottleneck, batch, scale trong giới hạn partition |
| Chỉ một partition lag cao | Hot key hoặc record đặc biệt nặng | Phân phối key/bytes theo partition | Đổi key/shard key hoặc tách workload; cân nhắc ordering |
| Rebalance liên tục | Pod restart, poll quá lâu, timeout sai | Consumer logs, rebalance rate, `max.poll.interval.ms` | Sửa health/resource, giảm batch, static membership/new protocol |
| Duplicate side effect | Crash sau xử lý trước commit | Event ID và thời điểm commit | Idempotent consumer, unique constraint, transaction DB |
| Record không được xử lý | Commit sớm, deserialize lỗi, retention hết | Committed offset, DLT, consumer logs | Sửa commit policy, DLT, replay từ offset hợp lệ |
| Producer timeout | Broker/network chậm, ISR thiếu, buffer đầy | Producer error, request latency, ISR, broker load | Khôi phục broker/network, backpressure, kiểm tra timeout |
| `NotEnoughReplicas` | ISR nhỏ hơn `min.insync.replicas` | ISR và broker health | Khôi phục replica; không hạ durability chỉ để hết lỗi |
| Disk gần đầy | Retention sai, lag/rebuild lớn, traffic tăng | Disk growth theo broker/topic | Mở rộng disk/broker, điều chỉnh retention, reassignment |
| DLT tăng nhanh | Poison pill, deploy breaking schema, downstream hỏng | Exception class và deploy timeline | Dừng retry storm, rollback/fix, redrive có kiểm soát |
| Streams restore lâu | State lớn, changelog chậm, thiếu standby | Restore lag, disk/network | Standby replica, capacity, tối ưu state/partition |

### 11.2 Quy Trình Khi Consumer Lag Tăng
1. Xác định lag tăng trên tất cả hay chỉ một partition.
2. Kiểm tra consumer instance có crash/restart/rebalance hay không.
3. Đo thời gian deserialize, business processing, DB/API và retry.
4. Kiểm tra producer rate có tăng đột biến hoặc backfill hay không.
5. Tạm dùng backpressure/circuit breaker để bảo vệ downstream.
6. Tối ưu hoặc scale consumer nếu còn partition trống; chỉ tăng partition sau khi capacity test.
7. Xác nhận lag đang giảm và freshness đã trở lại SLO.

### 11.3 Quy Trình DLT và Redrive
1. Dừng hoặc giới hạn redrive tự động khi chưa biết nguyên nhân.
2. Nhóm lỗi theo exception/schema/event type thay vì sửa từng record thủ công.
3. Sửa code hoặc dữ liệu và kiểm tra trên sample an toàn.
4. Redrive sang topic nguồn/retry topic với rate limit và giữ original metadata.
5. Dùng event ID cũ để idempotency tiếp tục có hiệu lực.
6. Theo dõi success/DLT/duplicate rate và lưu audit của lần replay.

### 11.4 Khi Nào Không Nên Dùng Kafka
- Request/response đồng bộ cần kết quả ngay và không cần buffering/event history.
- Queue nhỏ, đơn giản, không có nhu cầu replay hoặc stream processing; một message broker truyền thống có thể dễ vận hành hơn.
- Đội ngũ chưa có năng lực vận hành distributed storage nhưng workload chưa đủ lớn để biện minh chi phí.
- Dùng Kafka chỉ để thay REST giữa mọi service sẽ làm hệ thống phức tạp mà không tự động tạo loose coupling.

---

## 12. HIGH AVAILABILITY, DISASTER RECOVERY & NÂNG CẤP

### 12.1 Baseline Production
- Thông thường dùng replication factor 3 và `min.insync.replicas=2` cho topic quan trọng, sau khi đánh giá availability/durability trade-off.
- Phân bố replica theo broker/rack/AZ; không đặt mọi replica của partition trong cùng failure domain.
- Tắt unclean leader election cho dữ liệu cần durability, trừ khi chấp nhận mất dữ liệu để đổi lấy availability.
- KRaft controller quorum cần số voter lẻ, failure domain độc lập và monitoring riêng.
- Giữ capacity để cluster vẫn chịu tải và rebuild replica khi mất một broker.

### 12.2 Failure Drill Bắt Buộc
- Kill một consumer giữa lúc xử lý và xác nhận duplicate được xử lý an toàn.
- Kill leader broker và đo producer error/latency, thời gian bầu leader, ISR recovery.
- Mô phỏng DB/API downstream timeout để kiểm tra backoff, circuit breaker và retry storm.
- Mô phỏng disk gần đầy và xác minh alert/runbook.
- Khởi động lại Streams instance để đo state restore.
- Thực hành replay/DLT redrive trên môi trường staging.

### 12.3 Disaster Recovery
- Xác định rõ RPO và RTO trước khi chọn active-passive hay active-active.
- Dùng MirrorMaker 2 hoặc nền tảng replication tương đương cho cross-cluster; kiểm thử topic config, ACL, consumer offset và failover.
- Replication bất đồng bộ không bảo đảm RPO bằng 0. Sau failover vẫn có thể trùng hoặc thiếu một khoảng record.
- Thiết kế global event ID/idempotency để failover/failback không tạo side effect trùng.
- Kafka replication không thay thế hoàn toàn backup/audit store cho yêu cầu lưu trữ pháp lý.

### 12.4 Rolling Upgrade và Partition Reassignment
- Đọc compatibility guide của đúng phiên bản trước khi đổi broker/client/protocol.
- Upgrade từng broker và theo dõi ISR, offline partition, request error/latency sau mỗi bước.
- Partition reassignment tiêu tốn network/disk; dùng throttle và chạy ngoài peak.
- Không thay đồng thời broker version, security protocol, partition count và application topology trong một lần deploy.

---

## 13. KIỂM THỬ KAFKA

### 13.1 Test Pyramid
1. **Unit test:** Serializer, partition-key function, idempotency logic và error classification.
2. **Contract test:** Schema compatibility và producer/consumer contract trong CI.
3. **Topology test:** `TopologyTestDriver` cho Kafka Streams, bao gồm late/out-of-order event.
4. **Integration test:** Testcontainers với Kafka thật, database thật và Schema Registry/Connect nếu có.
5. **Failure/chaos test:** Kill broker/consumer, inject timeout, duplicate, malformed record và disk pressure.
6. **Load test:** Dùng record size/key distribution/replication/compression gần production, đo p95/p99 latency và recovery.

### 13.2 Kịch Bản Tối Thiểu Phải Có
- Cùng `eventId` được gửi hai lần nhưng business state chỉ thay đổi một lần.
- Consumer crash sau DB commit nhưng trước offset commit.
- Producer nhận timeout không rõ record đã ghi hay chưa.
- Schema mới tương thích và schema breaking bị CI từ chối.
- Poison pill không chặn vô hạn partition; DLT chứa đủ metadata.
- Event đến muộn trong grace được tính, event quá grace có metric/nhánh xử lý rõ ràng.
- Broker leader chết nhưng producer/consumer phục hồi trong SLO.
- Redrive DLT không tạo side effect trùng.

---

## 14. LỘ TRÌNH HỌC KAFKA 8 TUẦN

### Tuần 1 — Core Architecture
- Học broker, topic, partition, leader/follower, offset, ISR, segment và KRaft.
- Lab: Dùng CLI tạo topic, produce/consume, xem offset và mô tả replica/ISR.
- Hoàn thành khi có thể giải thích đường đi của một record từ producer tới consumer.

### Tuần 2 — Producer và Partitioning
- Học key, partitioner, batching, compression, ACK, retry, idempotence và timeout.
- Lab: So sánh throughput/latency theo `linger.ms`, compression và record size; thử tăng partition và quan sát key mapping.

### Tuần 3 — Consumer, Offset và Rebalance
- Học poll loop, manual commit, consumer group, assignment, lag và rebalance.
- Lab: Crash ở ba thời điểm trước xử lý, sau xử lý-trước commit, sau commit; ghi lại duplicate/loss.

### Tuần 4 — Reliability Patterns
- Học idempotent consumer, retry/DLT/redrive, Kafka transaction và Transactional Outbox.
- Lab: Unique constraint + business update trong một DB transaction; outbox publisher crash sau Kafka ACK.

### Tuần 5 — Schema và Data Governance
- Học Avro/Protobuf/JSON Schema, compatibility, subject strategy và event envelope.
- Lab: CI chặn schema breaking; rollout consumer trước rồi producer với field mới.

### Tuần 6 — Connect, CDC và Saga
- Học Connect worker/task, Debezium snapshot/log, outbox CDC, orchestration/choreography và compensation.
- Lab: DB change $\rightarrow$ Kafka; dừng connector rồi khởi động lại; compensation bị lỗi và retry.

### Tuần 7 — Kafka Streams
- Học KStream/KTable, event time, window/grace, state store, changelog và repartition.
- Lab: Fraud detection với out-of-order/late event; restart instance và đo state restore.

### Tuần 8 — Production Operations
- Học metrics/SLO, security, capacity, HA, DR, upgrade và reassignment.
- Lab cuối: Load test + kill broker/consumer + DLT redrive; viết postmortem và runbook.

### Tiêu Chí “Đã Hiểu Kafka”
Không chỉ chạy được happy path. Bạn phải giải thích và chứng minh bằng test được:
- Khi nào record có thể mất, trùng hoặc sai thứ tự.
- Offset đã commit có ý nghĩa gì và không có ý nghĩa gì.
- Vì sao `acks=all` chưa đủ nếu cấu hình replication/ISR sai.
- Exactly-once kết thúc ở đâu khi có DB/API bên ngoài.
- Cách điều tra lag/rebalance/DLT thay vì chỉ tăng partition.
- Cách phục hồi sau broker, consumer, connector hoặc Streams instance failure.

---

## 15. TÀI LIỆU CHÍNH THỨC NÊN ĐỌC

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Producer Configuration](https://kafka.apache.org/42/configuration/producer-configs/)
- [Consumer Configuration](https://kafka.apache.org/42/configuration/consumer-configs/)
- [Topic Configuration và `min.insync.replicas`](https://kafka.apache.org/42/configuration/topic-configs/)
- [Consumer Rebalance Protocol](https://kafka.apache.org/42/operations/consumer-rebalance-protocol/)
- [Kafka Streams DSL và Windowing](https://kafka.apache.org/42/streams/developer-guide/dsl-api/)
- [Kafka Tiered Storage](https://kafka.apache.org/42/operations/tiered-storage/)
- [Spring for Apache Kafka Reference](https://docs.spring.io/spring-kafka/reference/)

> Khi dự án chốt phiên bản Kafka/Spring Kafka, hãy đọc đúng bộ tài liệu cùng phiên bản thay vì copy cấu hình từ blog cũ.

---
*Tài liệu là lộ trình học và checklist vận hành. Mọi con số cấu hình cần được benchmark và kiểm chứng trên phiên bản Kafka, workload và hạ tầng thực tế.*
