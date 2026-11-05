# Làm việc trên GitHub Codespaces

Codespaces là máy ảo GitHub cấp sẵn, mở trong trình duyệt, đã có Docker và code của repo. Không cần cài WSL hay Docker trên máy mình.

Cấu hình nằm trong `.devcontainer/`: máy **4 nhân, 16 GB RAM**, có sẵn Docker, `make`, Python 3.10, Java 17, `pyspark` và `duckdb`.

## Chi phí: miễn phí trong hạn mức

Mỗi tài khoản GitHub cá nhân có một số **giờ lõi CPU (core-hours)** và **dung lượng lưu trữ** miễn phí mỗi tháng; tài khoản có GitHub Pro (sinh viên được tặng qua Student Developer Pack) được nhiều hơn. Máy 4 nhân tiêu 4 giờ lõi cho mỗi giờ chạy. Xem hạn mức và lượng đã dùng tại **Settings → Billing and plans**.

Để chắc chắn **không bị trừ tiền**:
- **Settings → Billing and plans → Spending limits**: giữ mức **0 USD** cho Codespaces (mặc định). Hết hạn mức thì codespace dừng, không tính tiền.
- **Settings → Codespaces → Default idle timeout**: đặt khoảng **30 phút**, để codespace tự dừng khi bỏ quên.
- Làm xong: `make down` rồi **Stop codespace**. Codespace đã dừng không tốn giờ lõi, chỉ tốn dung lượng lưu trữ.
- Xóa các codespace cũ không dùng nữa tại **github.com/codespaces**.

## Cài một lần: secrets

**Settings (tài khoản cá nhân) → Codespaces → Secrets → New secret**, chọn repo của nhóm:

| Tên | Giá trị |
|---|---|
| `GPS_MINIO_PASSWORD` | Mật khẩu MinIO (ít nhất 8 ký tự, không dùng ký tự `\|`) |
| `GPS_VEHICLE_KEY_SALT` | Chuỗi bí mật chung của **cả nhóm** (mọi người phải giống nhau) |

Khi tạo codespace, file `infra/.env` được tự sinh từ hai secret này.

## Mở codespace

Trên trang repo: **Code → Codespaces → ⋯ → New with options** → **Machine type: 4-core** → **Create codespace**. Lần đầu mất vài phút để cài đặt. Lần sau chỉ cần mở lại codespace cũ trong danh sách.

Trong terminal:
```bash
cd infra
make build        # lần đầu 10–20 phút
make up-core
make smoke
```

Xem giao diện: tab **PORTS** (cạnh **TERMINAL**) → bấm biểu tượng quả địa cầu ở dòng tương ứng (MinIO Console 9001, Spark Master UI 18080, Kibana 5601, Airflow 8088...). Các cổng mặc định ở chế độ **Private**: chỉ chính bạn mở được. **Không đổi sang Public**.

## Dữ liệu

Ổ đĩa codespace chỉ khoảng 32 GB, chia cho image Docker và dữ liệu. Vì vậy:
- **Chỉ đưa lên dữ liệu đã cắt nén** (`*.sql.gz`) hoặc **bộ mẫu**, không đưa file dump 30 GB.
- Tải từ máy mình lên bằng GitHub CLI (cài `gh` trên máy mình, đăng nhập bằng `gh auth login`):
  ```bash
  gh codespace cp -e -r data/raw/chunks 'remote:/workspaces/<tên-repo>/data/raw/'
  ```
  Hoặc kéo thả file vào cây thư mục bên trái (chỉ hợp với file nhỏ).
- Thư mục `data/` vẫn bị `.gitignore` loại trừ: dữ liệu nằm trong codespace của bạn, **không bị đẩy lên GitHub**.
- Hỏi bên cung cấp dữ liệu trước khi đưa dữ liệu thật lên Codespaces.

## Làm việc nhóm

Mỗi người có **codespace riêng** trên tài khoản của mình: MinIO, Spark... của ai người nấy dùng. Code chia sẻ qua Git như bình thường (nhánh + Pull Request), bộ mẫu dữ liệu chia sẻ qua file Parquet.

Buổi ghép nối: một người chạy trên codespace của mình và chia sẻ màn hình. Muốn sửa code cùng nhau trực tiếp thì dùng **Live Share** của VS Code.

## Giới hạn

- Codespace tự dừng khi để không, nên **không chạy được dịch vụ 24/7** và không làm máy chủ cho CI/CD tự triển khai. Phần CI (kiểm thử) vẫn chạy bình thường trên GitHub. Bước triển khai tự bỏ qua cho đến khi nhóm có máy chủ và đặt biến `DEPLOY_DIR`.
- Không bật tất cả profile cùng lúc nếu thấy chậm: 16 GB là vừa đủ cho phần lõi + một hai profile.
