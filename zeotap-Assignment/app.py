from flask import Flask, render_template, request, jsonify, send_file
import os
import pandas as pd
from clickhouse_connect import get_client
from werkzeug.utils import secure_filename

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

clickhouse_client = None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/connect', methods=['POST'])
def connect():
    data = request.json
    try:
        global clickhouse_client
        clickhouse_client = get_client(
            host=data['host'],
            port=data['port'],
            username=data['username'],
            password=data['jwt'],  # JWT as password
            secure=True
        )
        db_tables = clickhouse_client.query(f"SHOW TABLES FROM {data['database']}").result_rows
        tables = [table[0] for table in db_tables]
        return jsonify({'status': 'success', 'tables': tables})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/get_columns', methods=['POST'])
def get_columns():
    table = request.json['table']
    try:
        result = clickhouse_client.query(f'DESCRIBE TABLE {table}')
        columns = [row[0] for row in result.result_rows]
        return jsonify({'status': 'success', 'columns': columns})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/export', methods=['POST'])
def export():
    table = request.form['table']
    columns = request.form.getlist('columns[]')
    filename = f'{table}_export.csv'
    try:
        result = clickhouse_client.query(f"SELECT {', '.join(columns)} FROM {table}")
        df = pd.DataFrame(result.result_rows, columns=columns)
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        df.to_csv(filepath, index=False)
        return send_file(filepath, as_attachment=True)
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/import', methods=['POST'])
def import_csv():
    table = request.form['table']
    columns = request.form.getlist('columns[]')
    file = request.files['file']
    try:
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        df = pd.read_csv(filepath)
        df = df[columns]
        rows = [tuple(x) for x in df.values]
        cols = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        clickhouse_client.insert(f'INSERT INTO {table} ({cols}) VALUES', rows)
        return jsonify({'status': 'success', 'records': len(rows)})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    app.run(debug=True)
