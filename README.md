## Streamlit app kết nối Databricks

Ứng dụng đơn giản dùng Streamlit để kết nối tới Databricks, query dữ liệu và hiển thị bảng + biểu đồ.

### Cấu trúc thư mục

- `app.py`: mã nguồn chính của ứng dụng Streamlit  
- `requirements.txt`: danh sách thư viện Python cần cài  
- `.streamlit/secrets.toml`: file chứa thông tin kết nối Databricks (KHÔNG commit lên GitHub)

### Cài đặt và chạy (local)

```bash
pip install -r requirements.txt
streamlit run app.py
```

### Tạo `secrets.toml`

Trong thư mục `.streamlit/` tạo file `secrets.toml` với nội dung ví dụ:

```toml
[databricks]
server_hostname = "xxx"
http_path = "xxx"
access_token = "xxx"
```

File này đã được ignore trong `.gitignore`, nên sẽ không bị đẩy lên GitHub.


