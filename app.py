from flask import Flask, jsonify
import psycopg2
import requests
from datetime import datetime

app = Flask(__name__)

# Array untuk menyimpan data yang telah dikirim ke Power BI
sent_data_log = []

# Fungsi untuk mendapatkan data dari view dashboard_view_lalin_daily dalam skema a_union
def get_data_from_dashboard_view_lalin_daily():
    try:
        conn = psycopg2.connect(
            dbname="tcm_datalake",
            user="konsultan",
            password="c3nt3rofd4t4",
            host="10.1.3.106",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM a_union.dashboard_view_lalin_daily')
        data = cursor.fetchall()
        conn.close()
        return data
    except Exception as e:
        return str(e)

# Fungsi untuk mengirim data ke API Power BI
def send_to_powerbi(data_to_send):
    formatted_data = []
    for row in data_to_send:
        volume_lalin = float(row[0])
        date = row[1].strftime('%Y-%m-%dT%H:%M:%S.%fZ')  # Menggunakan strftime untuk mengonversi datetime.date ke string
        formatted_data.append({"volume_lalin": volume_lalin, "date": date})

    # Kirim data ke API Power BI
    api_url = "https://api.powerbi.com/beta/fbf770ac-3ae0-464d-9457-cb04ad0b06f5/datasets/0fac34b1-c77f-40c5-ba6e-2e3502ca66e2/rows?experience=power-bi&key=fZ%2BiGnBvTZ9X25m7sW4pjzSkfZQi0yrFEEY07rxEC1GcB7TT3bbr7Y8xgzlyP1sX0jMHtcxC8qneEbofBjjBIA%3D%3D"
    headers = {'Content-Type': 'application/json'}
    response = requests.post(api_url, json=formatted_data, headers=headers)

    if response.status_code == 200:
        print('Data berhasil dikirim ke Power BI')
    else:
        print('Gagal mengirim data ke Power BI')

# Fungsi untuk menghitung selisih data sebelumnya dan data yang baru
def calculate_difference(previous_data, current_data):
    # Membuat dictionary dari data sebelumnya
    previous_data_dict = dict(previous_data)
    
    # Membandingkan data sebelumnya dengan data yang baru
    difference_data = []
    for data in current_data:
        volume_lalin_new = data[0]
        date_new = data[1]
        
        # Jika data tidak ada di log atau ada perubahan volumenya, tambahkan ke data selisih
        if date_new not in previous_data_dict or volume_lalin_new != previous_data_dict[date_new]:
            difference_data.append(data)
    
    return difference_data

# Route untuk mendapatkan data dari view dashboard_view_lalin_daily dan mengirimkannya ke API Power BI
@app.route('/get_dashboard_view_lalin_daily', methods=['GET'])
def send_to_powerbi_endpoint():
    # Ambil data dari view
    dashboard_view_lalin_daily_data = get_data_from_dashboard_view_lalin_daily()

    # Mengirim data ke Power BI hanya jika ada data yang dikirimkan
    if isinstance(dashboard_view_lalin_daily_data, list) and dashboard_view_lalin_daily_data:
        # Menghitung selisih data dengan data sebelumnya
        difference_data = calculate_difference(sent_data_log, dashboard_view_lalin_daily_data)
        
        # Kirim data selisih ke Power BI
        if difference_data:
            print(f"Total volume lalin yang baru: {sum([data[0] for data in difference_data])}")
            print(f"Total volume lalin sebelumnya: {sum([data[0] for data in sent_data_log])}")
            print(f"Selisih volume lalin: {sum([data[0] for data in difference_data]) - sum([data[0] for data in sent_data_log])}")
            
            send_to_powerbi(difference_data)
            sent_data_log.extend(difference_data)
            return jsonify({'status': 'Data berhasil dikirim ke Power BI'})
        else:
            return jsonify({'status': 'Tidak ada data baru yang perlu dikirim ke Power BI'})
    else:
        return jsonify({'status': 'Tidak ada data yang dikirim ke Power BI'})

if __name__ == '__main__':
    # Menjalankan Flask pada host 0.0.0.0 (semua antarmuka) dan port 9011
    app.run(host='0.0.0.0', port=9011, debug=True)
