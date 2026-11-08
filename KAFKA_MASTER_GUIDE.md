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
10. [Top 10 Câu Hỏi Phỏng Vấn Kafka Kinh Điển (Kèm Đáp Án)](#10-top-10-câu-hỏi-phỏng-vấn-kafka-kinh-điển)

---

## 1. TỔNG QUAN & KIẾN TRÚC CỐT LÕI

### 1.1 Kafka là gì?
Apache Kafka là một **Distributed Event Streaming Platform** (Nền tảng truyền phát luồng sự kiện phân tán) phân tán, có khả năng chịu lỗi cao, lưu trữ bền vững trên đĩa cứng và hỗ trợ thông lượng (throughput) lên tới hàng triệu bản tin/giây với độ trễ cực thấp (< 10ms).

### 1.2 Bảng Thuật Ngữ Cốt Lõi
| Thuật ngữ | Khái niệm & Vai trò thực tế |
| :--- | :--- |
| **Broker** | Server Kafka chạy dịch vụ lưu trữ và phân phối message. Một cluster gồm nhiều Broker kết nối nhau. |
| **KRaft Mode** | Cơ chế đồng thuận siêu tốc thế hệ mới của Kafka (thay thế hoàn toàn ZooKeeper), đơn giản hóa việc vận hành. |
| **Topic** | Kênh phân loại dữ liệu logic (tương đương với một Table trong SQL hoặc một folder). |
| **Partition** | Đơn vị lưu trữ vật lý cơ sở bên trong Topic. Mỗi partition là một file **Commit Log tuần tự (Append-only)**. Nhờ chia nhiều partition, Kafka xử lý song song và scale ngang vô hạn. |
| **Offset** | Số nguyên tự tăng định danh duy nhất cho từng message trong một Partition cụ thể. Mang tính chất bất biến (immutable). |
| **Message Key** | Khóa định danh của message. Kafka dùng thuật toán hash (`murmur2(key) % num_partitions`) để đảm bảo: **Mọi message có cùng Key sẽ luôn vào cùng 1 Partition** (bảo đảm thứ tự FIFO cho entity đó). |
| **Consumer Group** | Nhóm gồm nhiều Consumer instances cùng chia sẻ việc đọc dữ liệu từ các partition của Topic để cân bằng tải. |
| **ISR (In-Sync Replicas)** | Danh sách các bản sao partition (Replicas) đang theo kịp Leader về dữ liệu. Message chỉ an toàn khi được commit vào đủ số lượng bản sao trong ISR. |

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
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

### 2.2 Quy Tắc Phân Chia Partition & Consumer Group
- **1 Partition chỉ được đọc bởi tối đa 1 Consumer trong cùng 1 Consumer Group** tại một thời điểm (để đảm bảo thứ tự).
- Nếu Topic có **3 Partitions**:
  - Nếu Group có **1 Consumer**: Consumer đó đọc cả 3 partitions.
  - Nếu Group có **3 Consumers**: Mỗi Consumer đọc chính xác 1 partition (Tối ưu nhất).
  - Nếu Group có **5 Consumers**: 2 Consumer sẽ rơi vào trạng thái nhàn rỗi (idle/standby).
- **Scale ngang:** Muốn tăng throughput xử lý song song $\rightarrow$ Phải tăng số Partitions của Topic trước, sau đó tăng số Consumer.

---

## 3. ĐỘ TIN CẬY & CÁC MẪU THIẾT KẾ DOANH NGHIỆP

```
          3 CẤP ĐỘ CAM KẾT PHÂN PHÁI (DELIVERY SEMANTICS)
          
1. At-Most-Once   : Commit offset trước khi xử lý -> Không bao giờ trùng, nhưng CÓ THỂ MẤT TIN.
2. At-Least-Once  : Xử lý xong mới commit offset -> KHÔNG BAO GIỜ MẤT TIN, nhưng CÓ THỂ BỊ TRÙNG LẶP.
3. Exactly-Once   : Kết hợp Idempotent Producer + Kafka Transaction -> CHÍNH XÁC 1 LẦN DUY NHẤT.
```

### 3.1 Idempotent Producer & Idempotent Consumer
- **Idempotent Producer (`enable.idempotence=true`):**
  - Khi Producer gửi message, Broker gán cho nó `ProducerId (PID)` và `Sequence Number`.
  - Nếu Broker đã lưu nhưng gặp timeout mạng khiến Producer gửi lại $\rightarrow$ Broker phát hiện Sequence Number bị trùng và tự động loại bỏ, không lưu 2 lần.
- **Idempotent Consumer (Deduplication Pattern):**
  - Mạng có thể bị retry nhiều lần khiến Consumer nhận 1 bản tin 2 lần.
  - **Giải pháp:** Sử dụng bảng `processed_events` trong Database với `eventId` làm Primary Key.
  ```java
  if (processedEventRepository.existsById(event.getEventId())) {
      log.warn("Event {} đã xử lý rồi -> Bỏ qua!", event.getEventId());
      return;
  }
  // Xử lý nghiệp vụ & lưu processed_events
  ```

### 3.2 Kafka Transactions & Exactly-Once Semantics (EOS)
- Sử dụng mô hình **Read-Process-Write** nguyên tử:
  - Consumer đọc từ `topic-A`.
  - Xử lý logic và ghi kết quả sang `topic-B`.
  - Commit offset của `topic-A` *bên trong cùng Transaction* với bước ghi sang `topic-B`.
- **Cấu hình then chốt:**
  - Producer: `transaction-id-prefix: tx-`
  - Downstream Consumer: `isolation.level = read_committed` (Broker sẽ giấu toàn bộ các message của transaction bị Abort/Rollback).

### 3.3 Transactional Outbox Pattern (Giải Quyết Dual-Write)
- **Vấn đề "Dual-Write":** Khi tạo Order, nếu vừa ghi DB vừa gửi Kafka:
  - Ghi DB xong, gửi Kafka bị rớt mạng $\rightarrow$ Mất message.
  - Gửi Kafka trước, ghi DB bị rollback $\rightarrow$ Bắn message "ma".
- **Giải pháp Outbox Pattern:**
  1. Ghi `Order` và `OutboxEvent` (status `PENDING`) vào Database trong **CÙNG 1 TRANSACTION SQL**.
  2. Một Background Worker hoặc Debezium CDC quét các event `PENDING` và đẩy sang Kafka.
  3. Sau khi Kafka xác nhận (ACK) $\rightarrow$ Cập nhật status thành `SENT`.
  $\Rightarrow$ **Đảm bảo 100% không bao giờ mất message.**

### 3.4 Dead Letter Queue (DLQ / DLT) & Exponential Backoff
- **Poison Pill:** Một message có định dạng hỏng hoặc vi phạm nghiệp vụ khiến Consumer quăng Exception liên tục và làm kẹt Partition.
- **Quy trình xử lý:**
  1. Cho phép Retry tối đa $N$ lần (ví dụ 3 lần) kèm thời gian giãn cách (Backoff).
  2. Nếu vẫn thất bại $\rightarrow$ Đẩy message sang topic `topic-name.DLT`.
  3. Consumer DLT ghi nhận message hỏng để gửi alert Slack/Telegram hoặc lưu DB cho kỹ sư tra cứu thủ công.

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
2. **Hopping / Sliding Window (Cửa sổ trượt chồng lấn):**
   - Cửa sổ kéo dài 5 phút nhưng dịch chuyển mỗi 1 phút một lần.
   - Dùng để tính giá trị trung bình động (Moving Average).
3. **Session Window (Cửa sổ theo phiên hoạt động):**
   - Nhóm các hành động dựa trên thời gian không hoạt động (Inactivity Gap).
   - Ví dụ: Một phiên lướt web của User kết thúc khi họ không click gì trong 30 phút.

### 4.3 Ứng Dụng: Phát Hiện Gian Lận (Fraud Detection)
```java
// Lắng nghe order-events -> gom nhóm theo user -> cửa sổ 30 giây
orderStream
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30)))
    .aggregate(...)
    .filter((user, stats) -> stats.getCount() >= 3 || stats.getTotal() >= 50_000_000)
    .to("fraud-alerts");
```

---

## 5. QUẢN LÝ HỢP ĐỒNG DỮ LIỆU (SCHEMA REGISTRY & APACHE AVRO)

### 5.1 Vì sao Doanh Nghiệp cấm dùng JSON thô trong Kafka?
- **Tốn băng thông:** Từng message JSON đều phải lặp lại key `{"orderId": "...", "amount": ...}`.
- **Dễ đổ vỡ downstream:** Nếu Producer đổi tên trường `amount` thành `price`, các Consumer cũ sẽ bị `null` hoặc crash hàng loạt.

### 5.2 Lợi Ích Của Schema Registry + Apache Avro
- Dữ liệu được nén thành **nhị phân (binary)** siêu nhẹ (giảm 60 - 80% dung lượng đĩa và mạng).
- **Schema Evolution (Tiến hóa hợp đồng):**
  - **Backward Compatibility:** Consumer dùng schema mới vẫn đọc được dữ liệu do Producer cũ tạo ra.
  - **Forward Compatibility:** Consumer dùng schema cũ vẫn đọc được dữ liệu do Producer mới tạo ra.
  - **Full Compatibility:** Đảm bảo tương thích cả hai chiều.

---

## 6. CHANGE DATA CAPTURE (CDC) & TÍCH HỢP HỆ THỐNG

### 6.1 Kafka Connect là gì?
Framework phân tán giúp kết nối Kafka với các hệ thống khác (Databases, S3, Elasticsearch) mà **hoàn toàn không cần viết code**:
- **Source Connector:** Kéo dữ liệu từ nguồn vào Kafka (e.g. Debezium, JDBC Source).
- **Sink Connector:** Đẩy dữ liệu từ Kafka ra đích (e.g. Elasticsearch Sink, Snowflake Sink).

### 6.2 Debezium CDC (Change Data Capture)
- Đọc trực tiếp **Transaction Log** ở tầng thấp nhất của DB (WAL của PostgreSQL hoặc Binlog của MySQL).
- Mọi câu lệnh `INSERT`, `UPDATE`, `DELETE` trên Database tự động biến thành sự kiện đẩy lên Kafka trong mili-giây.
- Ứng dụng: Tự động làm tươi Redis Cache, đồng bộ tìm kiếm sang Elasticsearch, hoặc chuyển dữ liệu về Data Warehouse.

---

## 7. TINH CHỈNH HIỆU NĂNG & VẬN HÀNH (PERFORMANCE TUNING & INTERNALS)

### 7.1 Tinh Chỉnh Producer (Tối Đa Hóa Throughput & Độ An Toàn)
- `acks=all`: Bắt buộc tất cả bản sao trong ISR xác nhận trước khi báo thành công (Độ an toàn cao nhất).
- `enable.idempotence=true`: Chống trùng lặp message khi retry.
- `compression.type=snappy` (hoặc `lz4`, `zstd`): Nén dữ liệu với CPU overhead cực thấp, tăng tốc độ truyền qua mạng lên gấp nhiều lần.
- `batch.size=16384` (16KB) & `linger.ms=20`: Cho phép Producer chờ tối đa 20ms để gom nhiều message thành một đợt gửi (batch) lớn, giảm số lượng kết nối mạng socket.

### 7.2 Tinh Chỉnh Consumer & Cơ Chế Rebalance
- **Consumer Lag:** Chỉ số quan trọng nhất khi vận hành:
  $$\text{Consumer Lag} = \text{Log End Offset} - \text{Current Consumer Offset}$$
  Nếu Lag tăng liên tục $\rightarrow$ Consumer đang bị quá tải, cần scale thêm Consumer hoặc tăng số Partition.
- `max.poll.interval.ms`: Khoảng thời gian tối đa giữa 2 lần gọi `poll()`. Nếu logic xử lý tốn nhiều thời gian hơn ngưỡng này, Kafka coi Consumer đã chết và kích hoạt Rebalance!
- **Cooperative Sticky Assignor (`CooperativeStickyAssignor`):** Cơ chế phân phối thế hệ mới. Khi có node tham gia/rời nhóm, nó chỉ chuyển giao các partition bị ảnh hưởng, **không dừng toàn bộ cả cluster** (không có hiện tượng "Stop-the-world").

### 7.3 Log Retention vs Log Compaction
- **Log Retention:** Xóa message theo thời gian (`log.retention.hours=168` - 7 ngày) hoặc theo kích thước (`log.retention.bytes`).
- **Log Compaction:** Kafka chỉ giữ lại **bản ghi mới nhất cho mỗi Key**, xóa các bản ghi cũ của Key đó. Biến Kafka thành một Key-Value Store vĩnh viễn (rất quan trọng cho changelog topic của KTable).

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

---

## 9. TỔNG HỢP 2 DỰ ÁN THỰC HÀNH ĐÃ XÂY DỰNG

### 📌 Dự án 1: `kafka-learning-project` (Hands-on từng khái niệm)
- **Thư mục:** `c:\Users\quang\Videos\kafka-learning-project`
- **6 Bài Lab:**
  - Lab 1: Gửi/nhận String message cơ bản (`BasicProducer`, `BasicConsumer`).
  - Lab 2: Serialize & Deserialize JSON POJO (`OrderEvent`).
  - Lab 3: Key-Based Partition Routing & bảo đảm thứ tự FIFO.
  - Lab 4: Consumer Groups & Cân bằng tải song song (`concurrency = 3`).
  - Lab 5: Dead Letter Queue (DLQ / DLT) với Retry Listener.
  - Lab 6: Kafka Transactions & Exactly-Once (`isolation.level = read_committed`).

### 📌 Dự án 2: `kafka-ecommerce-eventdriven` (Microservices Event-Driven & Saga Pattern v2.0)
- **Thư mục:** `c:\Users\quang\Videos\kafka-ecommerce-eventdriven`
- **Mô hình kiến trúc:** **Maven Multi-Module Microservices** (7 modules độc lập).
- **Các Dịch vụ thành phần:**
  - `common-contracts`: Thư viện Data Contracts & Event DTOs bất biến dùng chung.
  - `order-service` (Port 8081): Quản lý vòng đời đơn hàng, Transactional Outbox Pattern, Saga Coordinator.
  - `inventory-service` (Port 8082): Quản lý tồn kho thực tế, Idempotent Consumer, Saga Compensating Rollback (xả kho khi lỗi).
  - `payment-service` (Port 8083): Xử lý thanh toán, Idempotent Guard, kích hoạt lệnh bù trừ khi thẻ bị từ chối.
  - `fraud-detection-service` (Port 8084): Kafka Streams Engine độc lập (Sliding Window 30s) phát hiện gian lận và khóa đơn tự động.
  - `notification-service` (Port 8085): Tiếp nhận toàn bộ luồng sự kiện để mô phỏng gửi Email/SMS/Push cho khách.
  - `api-gateway` (Port 8080): Single Entry Point định tuyến toàn cụm và phục vụ Mission Control Web Dashboard.
- **Kỹ thuật thực chiến nâng cao:**
  - **Database-per-Service:** Mỗi service sở hữu database riêng biệt (`orderdb`, `inventorydb`, `paymentdb`), triệt tiêu phụ thuộc dữ liệu.
  - **Choreography-based Saga Pattern:** Phối hợp giao dịch phân tán không cần 2PC/XA.
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
> - `acks=1`: Producer chờ Leader Partition ghi vào đĩa thành công là báo OK (Mặc định). Nếu Leader chết trước khi sync sang Follower thì vẫn có thể mất dữ liệu.
> - `acks=all` (hoặc `-1`): Chờ cả Leader lẫn toàn bộ các bản sao trong danh sách ISR ghi thành công mới báo OK (An toàn nhất, chống mất dữ liệu tuyệt đối).

### Q4: Dual-Write Problem là gì và giải quyết thế nào với Kafka?
> **Trả lời:** Là tình huống một service cần vừa lưu dữ liệu vào Database, vừa gửi event lên Kafka. Vì 2 hệ thống này không chung transaction, lỗi mạng có thể làm một bên thành công còn bên kia thất bại (lệch dữ liệu). Giải pháp chuẩn ngành là dùng **Transactional Outbox Pattern**: lưu event vào bảng `outbox` trong cùng transaction SQL với dữ liệu chính, sau đó dùng Poller hoặc Debezium CDC đẩy lên Kafka.

### Q5: Consumer Lag là gì? Nếu Lag tăng vọt thì xử lý thế nào?
> **Trả lời:** Lag là độ chênh lệch giữa offset mới nhất trong topic và offset mà consumer đang đọc tới. Khi Lag tăng vọt:
> 1. Kiểm tra Consumer có bị chết hoặc bị dính lỗi timeout/rebalance không.
> 2. Tăng số Partition của Topic và tăng số Consumer tương ứng để scale ngang.
> 3. Tối ưu code xử lý trong Consumer (dùng thread pool hoặc batch processing).

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
> 3. Cho phép scale hàng triệu partition trong một cluster mà không bị nghẽn metadata tại ZooKeeper.

### Q11: Phân tán Giao Dịch trong Microservices bằng Saga Pattern thế nào?
> **Trả lời:** Trong Microservices, mỗi service có Database riêng nên không thể dùng `@Transactional` ACID truyền thống. Saga chia giao dịch lớn thành chuỗi các giao dịch cục bộ (Local Transactions). Khi một bước thất bại (ví dụ: thanh toán bị từ chối), hệ thống sẽ phát sinh **Compensating Transaction (Giao dịch bù trừ)** ngược lại các bước trước (ví dụ: xả kho đã giữ, hoàn điểm thưởng) để đảm bảo tính nhất quán cuối cùng (Eventual Consistency).

### Q12: So sánh Choreography Saga và Orchestration Saga?
> **Trả lời:**
> - **Choreography (Hướng sự kiện - Không có nhạc trưởng):** Các service tự lắng nghe event của nhau qua Kafka và tự quyết định hành động tiếp theo. Ưu điểm: Loose-coupling, hiệu năng cao, linh hoạt. Nhược điểm: Khó vẽ luồng tổng thể khi hệ thống có hàng chục bước.
> - **Orchestration (Có nhạc trưởng điều phối):** Dùng 1 Orchestrator tập trung (như Camunda, Temporal hoặc 1 Coordinator Service) gửi lệnh trực tiếp đến từng service. Ưu điểm: Dễ theo dõi và quản lý state tập trung. Nhược điểm: Tạo thành điểm nghẽn (Single Point of Failure / Bottleneck) nếu không thiết kế cẩn thận.

### Q13: Tại sao không nên chia sẻ chung Database giữa các Microservices?
> **Trả lời:** Chia sẻ chung Database vi phạm nguyên tắc bao đóng (loose coupling) của Microservices:
> 1. Khi 1 service đổi schema bảng, nó làm vỡ logic của tất cả các service khác đang đọc bảng đó.
> 2. Không thể chọn công nghệ lưu trữ phù hợp (ví dụ: Order dùng SQL, Analytics dùng ClickHouse/NoSQL).
> 3. Xung đột tài nguyên và khó phân quyền bảo mật độc lập.

### Q14: Khi scale out Outbox Publisher trên nhiều instance của Order Service thì giải quyết tranh chấp thế nào?
> **Trả lời:** Nếu chạy 3 pods của Order Service cùng quét bảng `outbox_events`:
> - Giải pháp 1: Dùng SQL `SELECT ... FOR UPDATE SKIP LOCKED` (chỉ khóa dòng chưa bị pod khác xử lý).
> - Giải pháp 2: Dùng **Debezium CDC** đọc Transaction Log của DB thay vì dùng Poller chủ động (chỉ cần 1 connector duy nhất, không lo tranh chấp).
> - Giải pháp 3: Dùng ShedLock hoặc Distributed Lock (Redis / Zookeeper) để chỉ định duy nhất 1 Pod làm Leader quét Outbox.

### Q15: Làm sao kiểm soát Event Versioning khi một Service cập nhật Event Payload?
> **Trả lời:**
> 1. Đặt tên Topic kèm version (ví dụ: `order-created-events.v1`, `order-created-events.v2`).
> 2. Áp dụng quy tắc tương thích ngược (Backward Compatibility): Không bao giờ xóa hoặc đổi tên trường cũ; trường mới luôn phải là optional (có giá trị mặc định).
> 3. Sử dụng **Schema Registry + Apache Avro/Protobuf** để tự động kiểm duyệt tính tương thích trước khi Producer được phép gửi bản tin.

---
*Tài liệu được biên soạn đồng bộ với 2 dự án thực hành đã tạo trong máy của bạn.*

