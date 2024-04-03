import psycopg2
from flask import Flask, jsonify
import requests
from datetime import datetime, date, timedelta
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
        cursor.execute('SELECT * FROM a_union.dashboard_view_lalin_daily')
        data = cursor.fetchall()
        conn.close()
        return data
    except Exception as e:
        return str(e)

def send_to_powerbi(data_to_send):
    api_url = "https://api.powerbi.com/beta/fbf770ac-3ae0-464d-9457-cb04ad0b06f5/datasets/02ed7006-7a71-4a3a-9661-bb575c1532ee/rows?experience=power-bi&key=qiS8cKbWMqEXWFkGBEWU%2BE2sxqIrNF8%2Fx5Gvese8VjzNirHlgqCgL82XkOisDvf1CqNZJ%2BQAB64fFWAp4uik2A%3D%3D"
    headers = {'Content-Type': 'application/json'}
    response = requests.post(api_url, json=data_to_send, headers=headers)

    if response.status_code == 200:
        print('Data successfully sent to Power BI')
    else:
        print('Failed to send data to Power BI:', response.text)



@app.route('/get_dashboard_view_lalin_daily', methods=['GET'])
def send_to_powerbi_endpoint():
    global last_date, last_volume

    try:
        # Mengambil data langsung dari database
        dashboard_view_lalin_daily_data = get_data_from_dashboard_view_lalin_daily()
        if isinstance(dashboard_view_lalin_daily_data, list) and dashboard_view_lalin_daily_data:
            # Mendapatkan tanggal dan volume lalin dari data yang diambil
            current_date = dashboard_view_lalin_daily_data[0][1]
            current_volume = float(dashboard_view_lalin_daily_data[0][0])

            print("Current Date:", current_date)
            print("Current Volume:", current_volume)

            if current_date != last_date or last_date is None:
                last_date = current_date
                print("Last Date:", last_date)
                last_volume = None  # Reset last_volume to None when entering a new day

                difference = current_volume  # Inisialisasi difference ke current_volume untuk kasus baru
                formatted_data = [{"volume_lalin": difference, "date": current_date.strftime('%d-%b-%Y')}]
                print("Formatted data (new day):", formatted_data)
                send_to_powerbi(formatted_data)
                return jsonify({'status': 'Data successfully sent to Power BI', 'volume_lalin_before': 0, 'volume_lalin_after': current_volume, 'volume_lalin': difference, 'date': current_date.strftime('%d-%b-%Y')})

            else:
                difference = 0  # Inisialisasi difference ke nilai 0 untuk kasus lainnya
                if last_volume is not None:
                    difference = current_volume - last_volume
                    if difference < 0:
                        difference = 0  # Kirim 0 untuk selisih negatif

                    formatted_data = [{"volume_lalin": difference, "date": current_date.strftime('%d-%b-%Y')}]
                    print("Difference:", difference)
                    print("Formatted data:", formatted_data)
                    send_to_powerbi(formatted_data)
                last_volume = current_volume  # Update last_volume setelah pengiriman

                return jsonify({'status': 'Data successfully sent to Power BI', 'volume_lalin_before': last_volume, 'volume_lalin_after': current_volume, 'volume_lalin': difference, 'date': current_date.strftime('%d-%b-%Y')})

    except Exception as e:
        print("Error:", e)
        return jsonify({'status': 'Failed to get data from database'})

    except Exception as e:
        print("Error:", e)
        return jsonify({'status': 'Failed to get data from database'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9011, debug=True)