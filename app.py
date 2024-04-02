import psycopg2
from flask import Flask, jsonify
import requests
from datetime import datetime, timedelta
import pytz

app = Flask(__name__)
last_date = None
last_volume = None

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
        cursor.execute('SELECT * FROM a_union.dashboard_view_lalin_daily ORDER BY "date" DESC LIMIT 1')
        data = cursor.fetchone()
        conn.close()
        return data
    except Exception as e:
        return str(e)

def send_to_powerbi(data_to_send):
    api_url = "https://api.powerbi.com/beta/fbf770ac-3ae0-464d-9457-cb04ad0b06f5/datasets/4995ff32-9db4-41d6-b565-5343755f08ef/rows?experience=power-bi&key=0oqW%2FvckvYnioZg7cVENyE%2FVlCoKhA84jYyPlydAvKGsO%2FE2e1MXWfnFATnSroXHZ2RnfjhPif3ze7YReMXVYQ%3D%3D"
    headers = {'Content-Type': 'application/json'}
    response = requests.post(api_url, json=data_to_send, headers=headers)

    if response.status_code == 200:
        print('Data successfully sent to Power BI')
    else:
        print('Failed to send data to Power BI:', response.text)

@app.route('/get_dashboard_view_lalin_daily', methods=['GET'])
def send_to_powerbi_endpoint():
    global last_date, last_volume

    current_date = datetime.now(pytz.timezone('Asia/Jakarta')).date()

    if current_date != last_date:
        last_date = current_date
        last_volume = None  # Reset last_volume setiap kali masuk hari baru

        # Ambil data dari dashboard untuk hari ini
        dashboard_view_lalin_daily_data = get_data_from_dashboard_view_lalin_daily()
        if dashboard_view_lalin_daily_data:
            new_volume, new_date = dashboard_view_lalin_daily_data
            formatted_data = [{"volume_lalin": new_volume, "date": new_date.strftime('%d-%m-%Y')}]
            print("Formatted data (new day):", formatted_data)
            send_to_powerbi(formatted_data)

        return jsonify({'status': 'Data successfully sent to Power BI'})
    
    else:
        # Jika masih dalam hari yang sama, hit API untuk mendapatkan data terbaru
        dashboard_view_lalin_daily_data = get_data_from_dashboard_view_lalin_daily()
        if dashboard_view_lalin_daily_data:
            new_volume, new_date = dashboard_view_lalin_daily_data

            if last_volume is not None:
                difference = new_volume - last_volume
                if difference < 0:
                    difference = 0  # Kirim 0 untuk selisih negatif

                formatted_data = [{"volume_lalin": difference, "date": new_date.strftime('%d-%m-%Y')}]
                print("Difference:", difference)
                print("Formatted data:", formatted_data)
                send_to_powerbi(formatted_data)
            last_volume = new_volume  # Update last_volume hanya setelah pengiriman

        return jsonify({'status': 'Data successfully sent to Power BI'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9011, debug=True)
