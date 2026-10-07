# Hướng dẫn chạy pipeline DNSE

Tài liệu này mô tả trạng thái hiện tại trên Windows: Airflow và HDFS chạy bằng Docker Compose, PostgreSQL 18 chạy trên Windows. Pipeline thu thập dữ liệu batch từ DNSE, lưu phản hồi JSON gốc vào HDFS Raw và trạng thái request vào PostgreSQL.


## 1. Kiến trúc hiện tại

DNSE API → Airflow → Collector → HDFS Raw (JSON gốc)
　　　　　　　　　　　　　　　　└→ PostgreSQL 18 (trạng thái/lỗi crawl)

HDFS gồm 1 NameNode và 2 DataNode container trên cùng laptop. Airflow dùng SequentialExecutor trong Docker Compose.

Hai DAG:

- dnse_market_universe_daily: DAG chính; lấy snapshot danh mục, OHLCV 1 phút cho các mã qua bộ lọc và VNINDEX/VN30.
- dnse_stock_ohlc_daily: DAG cũ cho watchlist 30 mã; nên để tạm dừng để tránh thu thập trùng.

## 2. Chuẩn bị PostgreSQL

Trong pgAdmin, tạo hai database trên PostgreSQL Windows tại localhost:5432:

- airflow: metadata Airflow.
- stock_pipeline: trạng thái crawl; collector tự tạo bảng crawl_runs.

Airflow trong Docker kết nối tới máy Windows qua host.docker.internal, không phải localhost. Nếu kết nối thất bại, kiểm tra listen_addresses, pg_hba.conf với scram-sha-256 cho mạng Docker và firewall TCP 5432 trên mạng riêng. Không mở truy cập cho mọi địa chỉ.

## 3. Cấu hình thông tin bí mật

Sao chép .env.example thành .env và điền mật khẩu PostgreSQL trong hai URL kết nối, DNSE_API_KEY và DNSE_API_SECRET. Không commit hoặc gửi file .env. Mật khẩu chứa ký tự đặc biệt trong URL cần được URL-encode.

Các biến chính:

| Biến | Ý nghĩa | Mặc định |
|---|---|---|
| DNSE_MARKET_MAX_SYMBOLS | Giới hạn mã; 0 nghĩa là không giới hạn | 0 |
| DNSE_MARKET_RESOLUTION | 1 là nến 1 phút, 1D là nến ngày | 1 |
| DNSE_MARKET_LOOKBACK_DAYS | Số ngày lịch cho mỗi lần chạy lịch | 1 |
| DNSE_INSTRUMENT_PAGE_SIZE | Số dòng mỗi trang danh mục | 100 |
| DNSE_INSTRUMENT_MAX_PAGES | Giới hạn an toàn số trang | 30 |
| DNSE_MARKET_STOCK_GROUP_IDS | Nhóm dùng để lọc cổ phiếu | ST |

Bộ lọc hiện tại yêu cầu mã ba chữ cái in hoa và securityGroupId=ST. Đây là tập mã ứng viên từ API, chưa xác nhận là toàn bộ cổ phiếu đang niêm yết.

## 4. Khởi động

Mở PowerShell tại thư mục project:

    docker compose up --build -d
    docker compose ps

Truy cập Airflow tại http://localhost:8080 và HDFS NameNode UI tại http://localhost:9870.

Xem log:

    docker compose logs --tail 100 airflow
    docker compose logs --tail 100 hdfs-namenode

Airflow metadata lưu trong PostgreSQL ngoài Docker; HDFS lưu trên named volumes của Docker.

## 5. Chạy DAG và backfill

Trong Airflow UI, mở DAGs, tìm dnse_market_universe_daily và bỏ Pause nếu cần. Nút Trigger DAG chạy ngay. Khi backfill, nhập cấu hình JSON ở phần Configuration JSON:

    {
      "from_date": "2026-07-08",
      "to_date": "2026-10-07",
      "resolution": "1",
      "max_symbols": 0,
      "max_pages": 30
    }

resolution=1 là nến 1 phút. Có thể giới hạn chạy thử bằng max_symbols, ví dụ 10. Hai ngày biên được truyền vào yêu cầu API.

DAG chính có lịch 17:30 từ thứ Hai đến thứ Sáu theo giờ Việt Nam. Lần chạy lịch lấy cửa sổ mặc định một ngày, không lặp backfill 90 ngày. DAG cũ dnse_stock_ohlc_daily dùng watchlist 30 mã và lịch 16:30; không bật đồng thời nếu không muốn phát sinh request trùng.

## 6. Kết quả backfill đã ghi nhận

Backfill toàn universe ngày 07/10/2026 hoàn thành với kết quả:

- 1.992 dòng danh mục qua 20 trang API.
- 1.399 mã cổ phiếu ứng viên, bao gồm 30 mã watchlist ban đầu.
- 2.416.004 nến 1 phút; 107 mã trả về mảng nến rỗng.
- VNINDEX và VN30: tổng 28.580 nến.
- 1.401/1.401 request OHLC thành công.
- 1.421 file Raw, khoảng 86,2 MiB dữ liệu logic; dung lượng vật lý có thể cao hơn do HDFS replication.

1.992 là số dòng snapshot danh mục, không phải số mã được lấy nến. 1.399 là số mã qua bộ lọc hiện tại, chưa phải danh sách toàn thị trường đã xác minh.

Thử yêu cầu 5 năm dữ liệu cho ACB nhưng API chỉ trả khoảng 90 ngày (khoảng 14.110 nến). Đây là quan sát từ lần thử, không phải giới hạn được DNSE công bố. Dữ liệu mẫu và thống kê nằm tại:

    data/samples/market_universe_full_1m_backfill/

## 7. Xem HDFS Raw và trạng thái crawl

Liệt kê file:

    docker compose exec hdfs-namenode hdfs dfs -ls -R /raw

Đọc file JSON theo đúng đường dẫn HDFS trong kết quả:

    docker compose exec hdfs-namenode hdfs dfs -cat '/raw/duong-dan-den-file.json'

Raw được phân vùng theo nguồn, loại dữ liệu, ngày ingest, mã và resolution. Ví dụ:

    /raw/source=dnse/entity=ohlc_minute/ingest_date=YYYY-MM-DD/symbol=ACB/resolution=1/...
    /raw/source=dnse/entity=ohlc_index_minute/ingest_date=YYYY-MM-DD/symbol=VNINDEX/resolution=1/...
    /raw/source=dnse/entity=instrument_master/ingest_date=YYYY-MM-DD/...

NameNode UI có thể liệt kê file nhưng xem trước qua trình duyệt có thể lỗi vì WebHDFS chuyển tiếp sang địa chỉ DataNode chỉ truy cập được trong mạng Docker. Dùng hdfs dfs -cat để đọc từ máy local.

Trong pgAdmin có thể xem 50 request gần nhất:

    SELECT dag_run_id, symbol, resolution, from_date, to_date,
           status, record_count, raw_uri, error_message
    FROM crawl_runs
    ORDER BY started_at DESC
    LIMIT 50;

Các JSON cũ từ giai đoạn lưu local nằm trong data/raw/. Các lần chạy hiện tại ghi Raw lên HDFS.

## 8. Dừng dịch vụ và giữ dữ liệu

    docker compose down

Lệnh này dừng container nhưng giữ named volumes HDFS và database PostgreSQL. Tránh docker compose down -v nếu cần giữ dữ liệu HDFS.

HDFS hiện là các container trên cùng laptop. K3d/Spark cần cấu hình đường mạng tới NameNode và DataNode; việc kết nối này chưa triển khai. Spark, Iceberg, Trino và Power BI là các phần tiếp theo.

## Tài liệu tham khảo

- Tài liệu API dữ liệu thị trường DNSE: https://developers.dnse.com.vn/docs/dnse/market-data/
