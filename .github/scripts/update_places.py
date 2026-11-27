# -*- coding: utf-8 -*-
"""
MICHI-SHIRUBE 施設データの更新（GitHub Actions で毎月1回動く）

1. Overture Maps の最新版を調べる
2. 指定した国の、よく使う種類（コンビニ・GS・駅など）だけを取り出す
3. Supabase の places_stage に入れて、places_swap で places に入れ替える

環境変数
  SUPABASE_URL          … https://xxxx.supabase.co
  SUPABASE_SECRET_KEY   … Supabase の secret キー（sb_secret_… か、旧 service_role キー）
  PLACES_COUNTRIES      … 国（カンマ区切り。例 "JP" や "JP,TW"）
  PLACES_MIN_CONFIDENCE … これより確かさが低い施設は入れない（0〜1、ふつう 0.5）
  DRY_RUN               … "true" なら Supabase に入れず、件数だけ出す
  OVERTURE_RELEASE      … 版を手で決めたい時だけ（例 2026-11-18.0）
"""
import csv, gzip, json, os, sys, time, urllib.request, urllib.error

import duckdb

OUT_DIR = 'places_out'
COUNTRIES = [c.strip().upper() for c in os.environ.get('PLACES_COUNTRIES', 'JP').split(',') if c.strip()]
MIN_CONF = float(os.environ.get('PLACES_MIN_CONFIDENCE', '0.5') or 0.5)
DRY_RUN = os.environ.get('DRY_RUN', 'false').strip().lower() == 'true'
SB_URL = os.environ.get('SUPABASE_URL', '').strip()
# 「https://xxxx.supabase.co/rest/v1/」のように後ろに何か付いていても、「https://xxxx.supabase.co」だけにする
if SB_URL:
    from urllib.parse import urlsplit
    _u = urlsplit(SB_URL if '://' in SB_URL else 'https://' + SB_URL)
    SB_URL = f'{_u.scheme}://{_u.netloc}'
SB_KEY = os.environ.get('SUPABASE_SECRET_KEY', '').strip()
BATCH = 2000

# 国ごとの範囲（xmin, ymin, xmax, ymax）。範囲で先にしぼってから、住所の国で確かめる
COUNTRY_BBOX = {
    'JP': (122.0, 20.0, 154.5, 46.0),
    'TW': (119.0, 21.5, 122.5, 26.5),
    'KR': (124.5, 33.0, 131.0, 38.7),
    'TH': (97.3, 5.5, 105.7, 20.5),
}

# 種類の決め方：Overture の分類の文字に、下の言葉がふくまれていたらその種類（上から順に判定）
CAT_RULES = [
    ('rest',    ['rest_area', 'rest_stop', 'service_area', 'highway_service', 'roadside_station']),
    ('station', ['train_station', 'railway_station', 'rail_station', 'metro_station', 'subway_station', 'light_rail_station']),
    ('fuel',    ['gas_station', 'fuel_station', 'petrol_station', 'filling_station']),
    ('ev',      ['ev_charging', 'charging_station', 'electric_vehicle_charging']),
    ('conveni', ['convenience_store', 'convenience']),
    ('onsen',   ['hot_spring', 'onsen', 'public_bath', 'bathhouse', 'bath_house']),
    ('hotel',   ['hotel', 'ryokan', 'motel', 'hostel', 'guest_house', 'resort', 'lodging']),
    ('sight',   ['tourist_attraction', 'landmark', 'monument', 'castle', 'viewpoint', 'scenic', 'observation_deck', 'museum']),
]


def log(*a):
    print(*a, flush=True)


def http_json(url, timeout=30):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8'))


def latest_release(con):
    rel = os.environ.get('OVERTURE_RELEASE', '').strip()
    if rel:
        return rel
    # S3 の中を見て一番新しい版
    rows = con.execute(
        "SELECT DISTINCT regexp_extract(file, 'release/([^/]+)/', 1) AS r "
        "FROM glob('s3://overturemaps-us-west-2/release/*/theme=places/type=place/*.parquet') ORDER BY r DESC LIMIT 1"
    ).fetchall()
    if not rows:
        raise SystemExit('Overture の版が見つからない')
    return rows[0][0]


def cat_case(kexpr):
    parts = []
    for cat, words in CAT_RULES:
        cond = ' OR '.join("strpos(%s, '%s') > 0" % (kexpr, w) for w in words)
        parts.append("WHEN %s THEN '%s'" % (cond, cat))
    return 'CASE ' + ' '.join(parts) + ' ELSE NULL END'


def build_query(path, country, bbox, variant):
    xmin, ymin, xmax, ymax = bbox
    # 分類の文字（新しい版は taxonomy / basic_category、古い版は categories）
    if variant['tax']:
        k = ("lower(concat_ws(',', basic_category, taxonomy.primary, "
             "array_to_string(taxonomy.hierarchy, ','), array_to_string(taxonomy.alternates, ',')))")
    else:
        k = "lower(concat_ws(',', categories.primary, array_to_string(categories.alternate, ',')))"
    name_en = "map_extract(names.common, 'en')[1]" if variant['common'] else 'NULL'
    closed = "AND coalesce(operating_status, '') <> 'permanently_closed'" if variant['status'] else ''
    # 住所の国で確かめる。日本は住所の国が無くても、名前に日本語（かな・漢字）があれば入れる
    country_cond = f"country = '{country}'"
    if country == 'JP':
        country_cond += " OR (country IS NULL AND regexp_matches(name, '[ぁ-んァ-ヶ一-龯]'))"
    return f"""
    WITH src AS (
      SELECT id,
             names.primary AS name,
             {name_en} AS name_en,
             addresses[1].country AS country,
             addresses[1].region AS region,
             addresses[1].locality AS locality,
             addresses[1].freeform AS freeform,
             (bbox.ymin + bbox.ymax) / 2 AS lat,
             (bbox.xmin + bbox.xmax) / 2 AS lng,
             sources[1].dataset AS src,
             {k} AS k
      FROM read_parquet('{path}')
      WHERE bbox.xmin >= {xmin} AND bbox.xmax <= {xmax}
        AND bbox.ymin >= {ymin} AND bbox.ymax <= {ymax}
        AND coalesce(confidence, 1) >= {MIN_CONF}
        {closed}
    )
    SELECT id, '{country}' AS country,
           CASE WHEN name LIKE '道の駅%' THEN 'rest' ELSE {cat_case('k')} END AS cat,
           name, name_en,
           CASE WHEN freeform IS NOT NULL AND locality IS NOT NULL AND strpos(freeform, locality) > 0 THEN freeform
                ELSE nullif(concat_ws(' ', region, locality, freeform), '') END AS addr,
           lat, lng, src
    FROM src
    WHERE ({country_cond}) AND name IS NOT NULL AND name <> ''
      AND (name LIKE '道の駅%' OR ({cat_case('k')}) IS NOT NULL)
    """


def extract(con, path, country):
    bbox = COUNTRY_BBOX.get(country)
    if not bbox:
        raise SystemExit(f'{country} の範囲が未登録（COUNTRY_BBOX に足す）')
    # 版によって列の形がちがうので、合う形を順に試す
    variants = [
        {'tax': True, 'common': True, 'status': True},
        {'tax': True, 'common': False, 'status': True},
        {'tax': True, 'common': False, 'status': False},
        {'tax': False, 'common': True, 'status': True},
        {'tax': False, 'common': False, 'status': False},
    ]
    last = None
    for v in variants:
        try:
            t0 = time.time()
            rows = con.execute(build_query(path, country, bbox, v)).fetchall()
            log(f'{country}: {len(rows)} 件（{time.time() - t0:.0f}秒）列の形 {v}')
            return rows
        except duckdb.Error as e:
            last = e
            log('この形では読めない:', v, '→', str(e).splitlines()[0][:200])
    raise SystemExit(f'取り出し失敗: {last}')


def sb_request(method, path, body=None, prefer=None):
    headers = {'apikey': SB_KEY, 'Content-Type': 'application/json'}
    if SB_KEY.startswith('eyJ'):          # 旧 service_role キー（JWT）
        headers['Authorization'] = 'Bearer ' + SB_KEY
    if prefer:
        headers['Prefer'] = prefer
    data = json.dumps(body, ensure_ascii=False).encode('utf-8') if body is not None else None
    req = urllib.request.Request(SB_URL + path, data=data, method=method, headers=headers)
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                txt = r.read().decode('utf-8')
                return json.loads(txt) if txt else None
        except urllib.error.HTTPError as e:
            msg = e.read().decode('utf-8', 'replace')[:500]
            if e.code >= 500 and attempt < 3:
                log(f'Supabase {e.code}、やり直し…'); time.sleep(5 * (attempt + 1)); continue
            raise SystemExit(f'Supabase エラー {e.code} {path}: {msg}')
        except urllib.error.URLError as e:
            if attempt < 3:
                log('つながらない、やり直し…', e); time.sleep(5 * (attempt + 1)); continue
            raise


def upload(country, rows):
    cols = ['id', 'country', 'cat', 'name', 'name_en', 'addr', 'lat', 'lng', 'src']
    sb_request('DELETE', f'/rest/v1/places_stage?country=eq.{country}', prefer='return=minimal')
    for i in range(0, len(rows), BATCH):
        chunk = [dict(zip(cols, r)) for r in rows[i:i + BATCH]]
        sb_request('POST', '/rest/v1/places_stage', chunk, prefer='return=minimal')
        log(f'  入れた {min(i + BATCH, len(rows))}/{len(rows)}')
    min_rows = max(100, int(len(rows) * 0.5))
    n = sb_request('POST', '/rest/v1/rpc/places_swap', {'p_country': country, 'p_min_rows': min_rows})
    log(f'{country}: places を入れ替えた（{n} 件）')


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    if not DRY_RUN and (not SB_URL or not SB_KEY):
        raise SystemExit('SUPABASE_URL と SUPABASE_SECRET_KEY を GitHub の Secrets に入れてください')
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    con.execute("SET s3_region='us-west-2';")
    rel = latest_release(con)
    path = f's3://overturemaps-us-west-2/release/{rel}/theme=places/type=place/*.parquet'
    log('Overture の版:', rel)
    if not DRY_RUN:
        log('Supabase:', SB_URL.split('//')[-1].split('.')[0][:4] + '…supabase.co')

    stats = [f'Overture release: {rel}', f'min confidence: {MIN_CONF}', '']
    for country in COUNTRIES:
        rows = extract(con, path, country)
        counts = {}
        for r in rows:
            counts[r[2]] = counts.get(r[2], 0) + 1
        stats.append(f'[{country}] total {len(rows)}')
        for c, n in sorted(counts.items(), key=lambda x: -x[1]):
            stats.append(f'  {c}: {n}')
        with gzip.open(os.path.join(OUT_DIR, f'places_{country}.csv.gz'), 'wt', encoding='utf-8', newline='') as f:
            w = csv.writer(f)
            w.writerow(['id', 'country', 'cat', 'name', 'name_en', 'addr', 'lat', 'lng', 'src'])
            w.writerows(rows)
        if DRY_RUN:
            log(f'{country}: DRY_RUN なので Supabase には入れない')
        else:
            upload(country, rows)
    txt = '\n'.join(stats)
    log('\n' + txt)
    with open(os.path.join(OUT_DIR, 'stats.txt'), 'w', encoding='utf-8') as f:
        f.write(txt + '\n')


if __name__ == '__main__':
    main()
