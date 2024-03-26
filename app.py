import psycopg2
from flask import Flask, jsonify
import requests
from datetime import datetime, date

app = Flask(__name__)
last_date = date.today()  # Menyimpan tanggal terakhir ketika data diperbarui
last_value = None

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


def send_to_powerbi(data_to_send):
    api_url = "https://api.powerbi.com/beta/fbf770ac-3ae0-464d-9457-cb04ad0b06f5/datasets/f7fa9f29-7795-47ea-852e-ac64f80208f2/rows?experience=power-bi&key=jti6mOoqP9jV4R3DBpTO%2FIQnUNScwWE6hDvTLKvyMwP1nrdRBHhh2Ph7U9OUYQhoqdt1fgJdDd2RjoKykWKKaA%3D%3D"
    headers = {'Content-Type': 'application/json'}
    response = requests.post(api_url, json=data_to_send, headers=headers)

    if response.status_code == 200:
        print('Data berhasil dikirim ke Power BI')
    else:
        print('Gagal mengirim data ke Power BI')


@app.route('/get_dashboard_view_lalin_daily', methods=['GET'])
def send_to_powerbi_endpoint():
    global last_date
    global last_value

    current_date = date.today()

    # Jika tanggal saat ini berbeda dari tanggal terakhir, maka reset last_value menjadi None
    if current_date != last_date:
        last_value = None
        last_date = current_date

    dashboard_view_lalin_daily_data = get_data_from_dashboard_view_lalin_daily()
    print("Data sebelumnya: ", last_value)
    print("Data sekarang: ", dashboard_view_lalin_daily_data)

    if isinstance(dashboard_view_lalin_daily_data, list) and dashboard_view_lalin_daily_data:
        new_value = float(dashboard_view_lalin_daily_data[0][0])
        if last_value is not None:
            difference = abs(new_value - float(last_value))
            formatted_data = [{"volume_lalin": difference, "date": datetime.now().strftime('%d-%b-%y')}]
            print("Selisih: ", difference)
            print("Format Data: ", formatted_data)
            send_to_powerbi(formatted_data)
        last_value = new_value
        return jsonify({'status': 'Data berhasil dikirim ke Power BI'})
    else:
        return jsonify({'status': 'Tidak ada data yang dikirim ke Power BI'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9011, debug=True)
