import json
import mysql.connector
from kafka import KafkaConsumer
from datetime import datetime

# Kết nối với MySQL
conn = mysql.connector.connect(
    host="127.0.0.1",
    port=3307,
    user="root",
    password="U;TDRI05+58BP0.?i7Q*_0jThz6Epj%:",
    database="toll_db"
)
cursor = conn.cursor()

# Kết nối đến Kafka
consumer = KafkaConsumer(
    'toll_data_topic',
    bootstrap_servers=['localhost:9092'],
    group_id='toll-consumer-group',
    auto_offset_reset='earliest',  # Bắt đầu từ đầu topic nếu không có offset
    enable_auto_commit=True,       # Tự động commit offset
    value_deserializer=lambda x: x.decode('utf-8')  # Giải mã bytes thành chuỗi
)

# Hàm chuyển đổi định dạng timestamp
def convert_timestamp(timestamp_str):
    try:
        # Chuyển đổi timestamp từ 'Mon Aug  2 11:07:12 2021' thành '2021-08-02 11:07:12'
        timestamp = datetime.strptime(timestamp_str, "%a %b %d %H:%M:%S %Y")
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        print(f"Lỗi khi chuyển đổi timestamp: {timestamp_str}, lỗi: {e}")
        return None

# Hàm xử lý dữ liệu
def process_message(message):
    try:
        # Lấy dữ liệu từ Kafka (đã được giải mã thành chuỗi)
        data = message.value
        
        # Thử phân tích dữ liệu là JSON, nếu không được thì coi là CSV
        try:
            record = json.loads(data)
        except json.JSONDecodeError:
            # Nếu không phải JSON, giả định là CSV
            fields = data.split(',')
            if len(fields) != 9:  # Đảm bảo đủ 9 cột
                print(f"Dữ liệu CSV không đủ cột: {data}")
                return
            record = {
                'rowid': int(fields[0]),
                'timestamp': fields[1],
                'anonymized_vehicle_number': int(fields[2]),
                'vehicle_type': fields[3],
                'number_of_axles': int(fields[4]),
                'tollplaza_id': int(fields[5]),
                'tollplaza_code': fields[6],
                'type_of_payment_code': fields[7],
                'vehicle_code': fields[8]
            }

        # Chuyển đổi timestamp
        timestamp = convert_timestamp(record['timestamp'])
        if not timestamp:
            return

        # Chuẩn bị dữ liệu cho MySQL
        row_data = (
            record['rowid'],
            timestamp,
            record['anonymized_vehicle_number'],
            record['vehicle_type'],
            record['number_of_axles'],
            record['tollplaza_id'],
            record['tollplaza_code'],
            record['type_of_payment_code'],
            record['vehicle_code']
        )

        # Câu lệnh SQL
        query = """
            INSERT INTO toll_data 
            (rowid, timestamp, anonymized_vehicle_number, vehicle_type, number_of_axles, 
             tollplaza_id, tollplaza_code, type_of_payment_code, vehicle_code) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            timestamp = VALUES(timestamp),
            anonymized_vehicle_number = VALUES(anonymized_vehicle_number),
            vehicle_type = VALUES(vehicle_type),
            number_of_axles = VALUES(number_of_axles),
            tollplaza_id = VALUES(tollplaza_id),
            tollplaza_code = VALUES(tollplaza_code),
            type_of_payment_code = VALUES(type_of_payment_code),
            vehicle_code = VALUES(vehicle_code)
        """
        cursor.execute(query, row_data)
        conn.commit()
        print(f"Đã xử lý dữ liệu: {data}")

    except Exception as e:
        print(f"Lỗi khi xử lý dữ liệu: {str(e)}")
        conn.rollback()  # Rollback nếu có lỗi

# Đọc dữ liệu từ Kafka
try:
    print("Bắt đầu đọc dữ liệu từ Kafka...")
    for message in consumer:
        process_message(message)
except KeyboardInterrupt:
    print("Đã dừng consumer bởi người dùng.")
finally:
    # Đóng kết nối
    cursor.close()
    conn.close()
    consumer.close()
    print("Đã đóng tất cả kết nối.")