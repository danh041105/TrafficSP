# PROVENANCE - ma da chay tren Google Colab ngay 2026-10-31 de trich xuat NYC DOT Traffic Speeds 2025.
# Luu lai nguyen van de truy xuat nguon goc; KHONG chay trong pipeline DW (du lieu da co).
# Ket qua Colab in ra: tong 12 714 844 dong, khong co CANH BAO thieu dong.
from google.colab import drive
drive.mount('/content/drive')

import os, io, time, datetime as dt
import pandas as pd, requests

OUT = '/content/drive/MyDrive/nyc_dot_2025'
os.makedirs(OUT, exist_ok=True)
URL = 'https://data.cityofnewyork.us/resource/i4gi-tjb9.csv'
TOKEN = ''   # dan App Token cua NYC Open Data vao day neu co (khong bat buoc)
H = {'X-App-Token': TOKEN} if TOKEN else {}

def call(url, params, tries=6):
    for i in range(tries):
        try:
            r = requests.get(url, params=params, headers=H, timeout=300)
            if r.status_code == 200:
                return r
            print('HTTP', r.status_code, r.text[:200])
            if r.status_code == 403:
                raise SystemExit('Bi chan 403 tu may chu Colab')
        except requests.RequestException as e:
            print('Loi mang:', e)
        time.sleep(2 ** i)
    raise RuntimeError('That bai sau nhieu lan thu')

total = 0
for m in range(1, 13):
    f = f'{OUT}/2025-{m:02d}.parquet'
    if os.path.exists(f):
        n = len(pd.read_parquet(f, columns=['link_id'])); total += n
        print('Da co', f, n); continue
    d, end, parts = dt.date(2025, m, 1), dt.date(2025 + m // 12, m % 12 + 1, 1), []
    while d < end:
        w = f"data_as_of >= '{d}T00:00:00' AND data_as_of < '{d + dt.timedelta(days=1)}T00:00:00'"
        df = pd.read_csv(io.StringIO(call(URL, {'$where': w, '$order': 'data_as_of,link_id', '$limit': 100000}).text), dtype=str)
        n = int(call(URL.replace('.csv', '.json'), {'$select': 'count(*) as n', '$where': w}).json()[0]['n'])
        if len(df) != n:
            print('CANH BAO thieu dong', d, len(df), '/', n)
        parts.append(df); d += dt.timedelta(days=1)
    mdf = pd.concat(parts, ignore_index=True)
    mdf['data_as_of'] = pd.to_datetime(mdf['data_as_of'])
    for c in ['speed', 'travel_time']:
        mdf[c] = pd.to_numeric(mdf[c], errors='coerce')
    mdf.to_parquet(f, compression='zstd', index=False)
    total += len(mdf)
    print('Xong', f, len(mdf), 'dong')

print('TONG 2025:', total, '| ky vong: 12714844')
