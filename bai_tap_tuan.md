# XÂY DỰNG DATA PIPELINE & RESTFUL API TRA CỨU THÔNG TIN

## 1. Mục tiêu bài tập
*   Hiểu và thực hành thiết kế Cơ sở dữ liệu quan hệ (Mô hình 1-N / Header - Detail).
*   Làm quen với framework Java Spring Boot để phát triển RESTful API.
*   Áp dụng các tiêu chuẩn thực tế khi làm API: Phân trang, Xử lý lỗi (Exception Handling), Viết tài liệu (Swagger), và Logging.

## 2. Luồng kiến trúc (Architecture Flow)
`Dữ liệu (Silver Layer)  == (Spark Job) ==>  Relational DB (Postgres/Oracle)  ==> (Spring Boot) ==  Người dùng gọi API`

## 3. Yêu cầu chi tiết

### Phần 1: Xử lý dữ liệu (Spark) & Thiết kế Database
1.  **Thiết kế Database:**
    *   Tạo 2 bảng có quan hệ **1 - N (Header - Detail)**. 
    *   Cần thêm những gì để tối ưu, log?
2.  **Spark Job:**
    *   Viết 1 job Spark đọc dữ liệu đầu vào (từ nơi lưu silver layer).
    *   Xử lý, transform và ghi (write) dữ liệu vào 2 bảng vừa thiết kế trong Database. Yêu cầu đảm bảo tính toàn vẹn dữ liệu (không bị mất data, đúng khoá ngoại).

### Phần 2: Xây dựng Backend API (Java Spring Boot)
Tạo một service Spring Boot cung cấp API tra cứu thông tin với các tiêu chuẩn sau:

1.  **Endpoint API:** `GET /api/v1/users/search` (hoặc tên endpoint tuỳ chọn cho phù hợp).
2.  **Tham số đầu vào (Query Parameters):**
    *   `keyword`: Từ khóa tìm kiếm (Có thể là chuỗi chứa `Key` hoặc `Name`).
    *   `page`: Số trang hiện tại (mặc định = 0).
    *   `size`: Số bản ghi trên 1 trang (mặc định = 10).
3.  **Cấu trúc dữ liệu trả về (Response JSON):**
    *   Dữ liệu trả về **bắt buộc phải là dạng Nested JSON** (Object lồng nhau), không được trả về data phẳng (flat) sau khi join bảng.
    *   *Cấu trúc mẫu:*
        ```json
        {
          "status": 200,
          "message": "Thành công",
          "data": {
            "content": [
              {
                "key": "BH123456",
                "fullName": "Nguyễn Văn A",
                "details": [
                   { "company": "Công ty X", "fromDate": "01/2020", "toDate": "12/2021" },
                   { "company": "Công ty Y", "fromDate": "01/2022", "toDate": "Nay" }
                ]
              }
            ],
            "pageNo": 0,
            "pageSize": 10,
            "totalElements": 25,
            "totalPages": 3
          }
        }
        ```
4.  **Xử lý ngoại lệ (Global Exception Handling):**
5.  **Tài liệu API (Swagger / OpenAPI):**
    *   Tích hợp thư viện `springdoc-openapi-ui` để tự động generate giao diện Swagger.
    *   Yêu cầu Leader có thể truy cập `http://localhost:8080/swagger-ui.html` để nhập params và test trực tiếp không cần Postman.
6.  **Logging:**
    *   Sử dụng thư viện log (Logback / SLF4J).
    *   Ghi log thông tin (INFO): Mỗi khi có request gọi vào API (log lại URL, params).
    *   Ghi log lỗi (ERROR): Khi có Exception xảy ra (in ra stacktrace để debug).

## 4. Tiêu chí Đánh giá (Definition of Done)
1. Spark Job chạy thành công, check DB đủ dữ liệu, quan hệ bảng đúng đắn.
2. API chạy trơn tru, search theo Tên hay Mã đều ra kết quả đúng chuẩn cấu trúc lồng nhau.
3. Chỉnh `size=2` và xem dữ liệu có bị cắt đúng 2 người / trang không.
4. Cố tình truyền tham số sai để xem API có trả về mã lỗi 400 và cấu trúc thông báo lỗi thân thiện không.
5. Truy cập được Swagger UI và gọi thử thành công.