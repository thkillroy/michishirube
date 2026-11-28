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
  N02_URL               … 日本の駅データ（国土数値情報 鉄道）の zip を手で決めたい時だけ
  PLACES_MODE           … "count_food" なら飲食店の数を数えるだけ（Supabase には何もしない）

日本だけ、次のデータを足す（どれも商用利用できるもの）
  ・駅 … 国土数値情報（鉄道 N02、CC BY 4.0）。Overture の駅は捨てる（ホーム・出口・店や、遠くにずれた「東京駅」が多いため）
  ・IC・スマートIC・JCT … 国土数値情報（高速道路時系列 N06、今あるものだけ＝現況として使う時は CC BY 4.0）
  ・病院 … 国土数値情報（医療機関 P04、CC BY 4.0）。診療所・歯科は数が多すぎるので入れない
  ・空港 … 国土数値情報（空港 C28、商用可）
  ・道の駅・SA・PA・温泉・山・滝・湖・城・神社・寺・博物館・美術館・動物園・水族館・遊園地・塔・公園・観光地（日本語の別名も） … Wikidata（CC0）
    Overture に同じ名前で300m以内のものがある時は Wikidata 側を捨てる（二重にしない）
  どれかが読めなかった時は、その分だけ足さずに続ける（駅は Overture のまま）
"""
import csv, gzip, io, json, math, os, re, sys, time, unicodedata, urllib.parse, urllib.request, urllib.error, zipfile
from datetime import date

import duckdb

OUT_DIR = 'places_out'
COUNTRIES = [c.strip().upper() for c in os.environ.get('PLACES_COUNTRIES', 'JP').split(',') if c.strip()]
MIN_CONF = float(os.environ.get('PLACES_MIN_CONFIDENCE', '0.5') or 0.5)
DRY_RUN = os.environ.get('DRY_RUN', 'false').strip().lower() == 'true'
MODE = os.environ.get('PLACES_MODE', 'update').strip().lower() or 'update'
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
    ('onsen',   ['hot_spring', 'onsen', 'public_bath', 'bathhouse', 'bath_house', 'sauna']),
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


# ---------------------------------------------------------------------
# 日本の駅：国土数値情報（鉄道 N02）
# ---------------------------------------------------------------------
N02_SRC = 'MLIT N02'


def ksj_download(code, first_yy, url_env=None):
    """国土数値情報の zip を、一番新しい年から探して取ってくる（今年 → 去年 → … first_yy 年まで）"""
    urls = []
    if url_env and os.environ.get(url_env, '').strip():
        urls.append(os.environ[url_env].strip())
    for y in range(date.today().year, 2000 + first_yy - 1, -1):
        yy = y % 100
        urls.append(f'https://nlftp.mlit.go.jp/ksj/gml/data/{code}/{code}-{yy:02d}/{code}-{yy:02d}_GML.zip')
    for u in urls:
        try:
            req = urllib.request.Request(u, headers={'User-Agent': 'michishirube-places-update'})
            with urllib.request.urlopen(req, timeout=180) as r:
                data = r.read()
            if data[:2] == b'PK':
                log(f'国土数値情報 {code}:', u, f'{len(data) // 1024}KB')
                return data, u
        except Exception as e:
            log('  読めない:', u, str(e)[:120])
    return None, None


def n02_download():
    return ksj_download('N02', 24, 'N02_URL')


def _decode(b):
    for enc in ('utf-8-sig', 'cp932'):
        try:
            return b.decode(enc)
        except UnicodeDecodeError:
            pass
    return b.decode('utf-8', 'replace')


def _mid(geom):
    """点ならそのまま、線・面なら座標の平均（真ん中あたりの点）"""
    t = geom.get('type')
    cs = geom.get('coordinates') or []
    if t == 'Point':
        return cs[0], cs[1]
    if t == 'MultiLineString' or t == 'Polygon':
        cs = [c for line in cs for c in line]
    elif t == 'MultiPolygon':
        cs = [c for poly in cs for ring in poly for c in ring]
    elif t == 'MultiPoint':
        pass
    if not cs:
        return None
    xs = [c[0] for c in cs]; ys = [c[1] for c in cs]
    return sum(xs) / len(xs), sum(ys) / len(ys)


def ksj_features(zbytes, want, avoid=()):
    """zip の中から、名前に want（どれか）をふくみ avoid をふくまないファイルを読む（GeoJSON 優先、無ければシェープファイル）"""
    z = zipfile.ZipFile(io.BytesIO(zbytes))
    names = z.namelist()

    def ok(n):
        l = n.lower().replace('\\', '/').split('/')[-1]
        return any(w.lower() in l for w in want) and not any(a.lower() in l for a in avoid)
    gj = [n for n in names if n.lower().endswith('.geojson') and ok(n)]
    gj.sort(key=lambda n: (0 if 'utf' in n.lower() else 1, len(n), n))
    if gj:
        log('  読むファイル:', gj[0])
        js = json.loads(_decode(z.read(gj[0])))
        return [(f.get('properties') or {}, f.get('geometry') or {}) for f in js.get('features', [])]
    shp = [n for n in names if n.lower().endswith('.shp') and ok(n)]
    shp.sort(key=lambda n: (0 if 'utf' in n.lower() else 1, len(n), n))
    if shp:
        import shapefile  # pyshp
        base = shp[0][:-4]
        log('  読むファイル:', shp[0])
        for enc in ('cp932', 'utf-8'):
            try:
                out = []
                rd = shapefile.Reader(shp=io.BytesIO(z.read(base + '.shp')), shx=io.BytesIO(z.read(base + '.shx')),
                                      dbf=io.BytesIO(z.read(base + '.dbf')), encoding=enc)
                for sr in rd.iterShapeRecords():
                    out.append((sr.record.as_dict(), sr.shape.__geo_interface__))
                return out
            except UnicodeDecodeError:
                pass
        return []
    raise RuntimeError(f'zip の中に {want} のファイルが無い: ' + ', '.join(names[:30]))


def n02_features(zbytes):
    return ksj_features(zbytes, ['station'])


def n02_rows(feats):
    """同じ駅（グループコード＋駅名）を1件にまとめる。路線名・会社名は住所の欄に入れる（検索で当たるように）"""
    groups = {}
    for props, geom in feats:
        name = str(props.get('N02_005') or '').strip()
        if not name:
            continue
        m = _mid(geom)
        if not m:
            continue
        g = str(props.get('N02_005g') or props.get('N02_005c') or name)
        key = (g, name)
        e = groups.setdefault(key, {'xs': [], 'ys': [], 'lines': [], 'code': g})
        e['xs'].append(m[0]); e['ys'].append(m[1])
        line = str(props.get('N02_003') or '').strip()
        comp = str(props.get('N02_004') or '').strip()
        label = (comp + ' ' + line).strip() if comp and line and comp not in line else (line or comp)
        if label and label not in e['lines']:
            e['lines'].append(label)
    rows = []
    for (g, name), e in groups.items():
        nm = name if name.endswith('駅') else name + '駅'
        lng = sum(e['xs']) / len(e['xs']); lat = sum(e['ys']) / len(e['ys'])
        addr = '・'.join(e['lines'])[:300] or None
        # 知名度：乗り入れている路線・会社の数が多いほど高い（東京駅・新宿駅など）
        pop = min(1.0, 0.15 + 0.08 * len(e['lines']))
        rows.append((f'n02:{g}:{name}', 'JP', 'station', nm, None, addr, lat, lng, N02_SRC, pop))
    return rows


def replace_jp_stations(rows):
    """Overture の駅を国土数値情報の駅に入れ替える（読めなかった時はそのまま）"""
    try:
        zb, url = n02_download()
        if not zb:
            log('国土数値情報が読めないので、日本の駅は Overture のまま')
            return rows, None
        st = n02_rows(n02_features(zb))
    except Exception as e:
        log('国土数値情報の読み込みで失敗、日本の駅は Overture のまま:', str(e)[:300])
        return rows, None
    if len(st) < 5000:
        log(f'国土数値情報の駅が少なすぎる（{len(st)}件）ので使わない。日本の駅は Overture のまま')
        return rows, None
    kept = [r for r in rows if r[2] != 'station']
    log(f'日本の駅：Overture {len(rows) - len(kept)} 件 → 国土数値情報 {len(st)} 件')
    return kept + st, url


# ---------------------------------------------------------------------
# IC・スマートIC・JCT：国土数値情報（高速道路時系列 N06）
#   今あるもの（設置期間の終わりが無い／9999）だけを使う ＝ 現況データとしての利用（CC BY 4.0）
# ---------------------------------------------------------------------
N06_KIND = {'1': 'IC', '2': 'スマートIC', '3': 'JCT'}   # 4：その他の接合部は入れない


def _code(v):
    v = str(v if v is not None else '').strip()
    return v[:-2] if v.endswith('.0') else v


def n06_rows():
    zb, url = ksj_download('N06', 24)
    if not zb:
        return [], None
    feats = ksj_features(zb, ['joint'])
    rows, seen, kinds, ended = [], set(), {}, 0
    for props, geom in feats:
        end = _code(props.get('N06_014'))
        if end and end not in ('9999', '0'):
            ended += 1
            continue
        kind = N06_KIND.get(_code(props.get('N06_019')))
        kinds[_code(props.get('N06_019'))] = kinds.get(_code(props.get('N06_019')), 0) + 1
        if not kind:
            continue
        name = str(props.get('N06_018') or '').strip()
        m = _mid(geom)
        if not name or not m:
            continue
        nm = name
        if kind == 'JCT' and not re.search(r'(JCT|ＪＣＴ|ジャンクション)$', nm):
            nm += 'JCT'
        elif kind == 'スマートIC' and not re.search(r'(スマートIC|スマートＩＣ|スマートインターチェンジ)$', nm):
            nm += 'スマートIC'
        elif kind == 'IC' and not re.search(r'(IC|ＩＣ|インターチェンジ|インター)$', nm):
            nm += 'IC'
        key = (nm, round(m[0], 2), round(m[1], 2))
        if key in seen:
            continue
        seen.add(key)
        rows.append((f'n06:{nm}:{m[1]:.4f}:{m[0]:.4f}', 'JP', 'ic', nm, None, kind, m[1], m[0], 'MLIT N06', 0.2))
    log(f'  IC・JCT：{len(rows)} 件（終わった設置期間 {ended} 件は入れない／種別ごとの数 {kinds}）')
    return rows, url


# ---------------------------------------------------------------------
# 病院：国土数値情報（医療機関 P04、CC BY 4.0）
# ---------------------------------------------------------------------
def p04_rows():
    zb, url = ksj_download('P04', 20)
    if not zb:
        return [], None
    feats = ksj_features(zb, ['p04'], avoid=['_01_', '_02_'])
    rows, classes = [], {}
    for props, geom in feats:
        cl = _code(props.get('P04_001'))
        classes[cl] = classes.get(cl, 0) + 1
        if cl != '1':          # 1：病院 だけ（診療所・歯科は入れない）
            continue
        name = str(props.get('P04_002') or '').strip()
        m = _mid(geom)
        if not name or not m:
            continue
        addr = str(props.get('P04_003') or '').strip()
        if _code(props.get('P04_009')) == '1':
            addr = (addr + ' 救急告示病院').strip()
        pop = 0.25 if _code(props.get('P04_009')) == '1' else 0.15
        rows.append((f'p04:{name}:{m[1]:.5f}:{m[0]:.5f}', 'JP', 'hospital', name, None, addr or None, m[1], m[0], 'MLIT P04', pop))
    log(f'  病院：{len(rows)} 件（医療機関分類ごとの数 {classes}）')
    return rows, url


# ---------------------------------------------------------------------
# 空港：国土数値情報（空港 C28、商用可）
#   名前は「空港」（面）にあり、位置は「標点」（点）にあるので、空港IDでつなぐ
# ---------------------------------------------------------------------
def c28_rows():
    zb, url = ksj_download('C28', 21)
    if not zb:
        return [], None
    polys = ksj_features(zb, ['airport'], avoid=['reference', 'point'])
    try:
        pts = ksj_features(zb, ['reference', 'point'])
    except RuntimeError:
        pts = []
    ptmap = {}
    for props, geom in pts:
        m = _mid(geom)
        if m:
            ptmap[_code(props.get('C28_000'))] = m
    rows, seen = [], set()
    for props, geom in polys:
        name = str(props.get('C28_005') or '').strip()
        aid = _code(props.get('C28_000'))
        m = ptmap.get(aid) or _mid(geom)
        if not name or not m or name in seen:
            continue
        seen.add(name)
        nm = name if re.search(r'(空港|飛行場|ヘリポート)$', name) else name + '空港'
        rows.append((f'c28:{aid or name}', 'JP', 'airport', nm, None, None, m[1], m[0], 'MLIT C28', 0.6))
    log(f'  空港：{len(rows)} 件')
    return rows, url


# ---------------------------------------------------------------------
# Wikidata（CC0）：道の駅・SA・PA・温泉・観光地
# ---------------------------------------------------------------------
WD_UA = 'MICHI-SHIRUBE-places-update/1.0 (https://thkilroy.com/michishirube/)'
# (種類, Wikidata の QID か、日本語のラベルで探す時は None, ラベル)
WD_CLASSES = [
    ('rest',  None,        '道の駅'),
    ('rest',  None,        'サービスエリア'),
    ('rest',  None,        'パーキングエリア'),
    ('onsen', 'Q177380',   '温泉'),
    ('sight', 'Q8502',     '山'),
    ('sight', 'Q34038',    '滝'),
    ('sight', 'Q23397',    '湖'),
    ('sight', 'Q23413',    '城'),
    ('sight', 'Q845945',   '神社'),
    ('sight', 'Q5393308',  '仏教寺院'),
    ('sight', 'Q33506',    '博物館'),
    ('sight', 'Q207694',   '美術館'),
    ('sight', 'Q43501',    '動物園'),
    ('sight', 'Q2281788',  '水族館'),
    ('sight', 'Q194195',   '遊園地'),
    ('sight', None,        '展望台'),
    ('sight', 'Q12518',    '塔'),
    ('sight', 'Q22698',    '公園'),
    ('sight', 'Q570116',   '観光地'),
]
WD_ALT_MAX = 8          # 別名は1か所につき8個まで


def wd_get(url, timeout=90):
    req = urllib.request.Request(url, headers={'User-Agent': WD_UA, 'Accept': 'application/sparql-results+json'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8'))


def wd_find_class(label):
    """日本語のラベルで Wikidata の項目を探す（一番上の候補）"""
    q = urllib.parse.urlencode({'action': 'wbsearchentities', 'search': label, 'language': 'ja',
                                'uselang': 'ja', 'type': 'item', 'limit': 3, 'format': 'json'})
    js = wd_get('https://www.wikidata.org/w/api.php?' + q)
    hits = js.get('search') or []
    for h in hits:
        if h.get('label') == label:
            return h['id'], h.get('description', '')
    return (hits[0]['id'], hits[0].get('description', '')) if hits else (None, '')


def wd_subclasses(qid):
    """その分類の下にある細かい分類の一覧（自分もふくむ）"""
    sparql = f'SELECT DISTINCT ?c WHERE {{ ?c wdt:P279* wd:{qid} . }}'
    url = 'https://query.wikidata.org/sparql?' + urllib.parse.urlencode({'query': sparql, 'format': 'json'})
    bs = wd_get(url, timeout=120).get('results', {}).get('bindings', [])
    return [b['c']['value'].rsplit('/', 1)[-1] for b in bs] or [qid]


def wd_query(qid, deep=True, classes=None):
    if classes:
        path = 'wdt:P31 ?cls'
        target = 'VALUES ?cls { ' + ' '.join('wd:' + c for c in classes) + ' }\n  ?item ' + path
    else:
        target = '?item ' + ('wdt:P31/wdt:P279*' if deep else 'wdt:P31') + f' wd:{qid}'
    sparql = f"""
SELECT ?item ?ja ?en ?coord ?adminJa ?sl WHERE {{
  {target} ; wdt:P17 wd:Q17 ; wdt:P625 ?coord ; rdfs:label ?ja ; wikibase:sitelinks ?sl .
  FILTER(LANG(?ja) = "ja")
  OPTIONAL {{ ?item rdfs:label ?en . FILTER(LANG(?en) = "en") }}
  OPTIONAL {{ ?item wdt:P131 ?adm . ?adm rdfs:label ?adminJa . FILTER(LANG(?adminJa) = "ja") }}
  FILTER NOT EXISTS {{ ?item wdt:P576 ?gone }}
}}"""
    url = 'https://query.wikidata.org/sparql?' + urllib.parse.urlencode({'query': sparql, 'format': 'json'})
    return wd_get(url, timeout=120).get('results', {}).get('bindings', [])


def wd_alt_labels(items):
    """Wikidata の日本語の別名（例：鹿苑寺 → 金閣寺）。200件ずつまとめて聞く。戻り値：項目 → 別名のリスト"""
    out = {}
    for i in range(0, len(items), 200):
        vals = ' '.join('wd:' + q for q in items[i:i + 200])
        sparql = f'SELECT ?item ?alt WHERE {{ VALUES ?item {{ {vals} }} ?item skos:altLabel ?alt . FILTER(LANG(?alt) = "ja") }}'
        url = 'https://query.wikidata.org/sparql'
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, data=urllib.parse.urlencode({'query': sparql, 'format': 'json'}).encode(),
                                             headers={'User-Agent': WD_UA, 'Accept': 'application/sparql-results+json',
                                                      'Content-Type': 'application/x-www-form-urlencoded'})
                with urllib.request.urlopen(req, timeout=120) as r:
                    bs = json.loads(r.read().decode('utf-8')).get('results', {}).get('bindings', [])
                for b in bs:
                    q = b['item']['value'].rsplit('/', 1)[-1]
                    a = (b.get('alt', {}).get('value') or '').strip()
                    if a:
                        out.setdefault(q, []).append(a)
                break
            except Exception as e:
                if attempt == 2:
                    log('    別名の一部が読めない', str(e)[:100])
                time.sleep(3 * (attempt + 1))
        time.sleep(1)
    return out


def _alt_join(name, alts):
    """別名を空白でつなぐ（名前と同じもの・重なるものは外す）"""
    seen, keep = {_nkey(name)}, []
    for a in alts:
        k = _nkey(a)
        if k and k not in seen and len(a) <= 40:
            seen.add(k); keep.append(a.replace(' ', '').replace('　', ''))
        if len(keep) >= WD_ALT_MAX:
            break
    return ' '.join(keep) or None


def _pt(wkt):
    m = re.match(r'Point\(([-0-9.eE]+) ([-0-9.eE]+)\)', wkt or '')
    return (float(m.group(1)), float(m.group(2))) if m else None


def wikidata_rows():
    rows, seen, info = [], set(), []
    for cat, qid, label in WD_CLASSES:
        try:
            if not qid:
                qid, desc = wd_find_class(label)
                if not qid:
                    info.append(f'{label}: 見つからない'); continue
                info.append(f'{label}: {qid}（{desc}）')
            else:
                info.append(f'{label}: {qid}')
            try:
                bs = wd_query(qid, deep=True)
            except Exception as e:
                # 重すぎて止まった時：細かい分類の一覧を取り、30個ずつに分けて探す
                log(f'  {label}：一度に探すと重いので、細かい分類ごとに分けて探す', str(e)[:100])
                time.sleep(5)
                bs = []
                try:
                    subs = wd_subclasses(qid)
                    log(f'    細かい分類 {len(subs)} 個')
                    for i in range(0, len(subs), 30):
                        try:
                            bs += wd_query(qid, classes=subs[i:i + 30])
                        except Exception as e2:
                            log('    一部読めない', str(e2)[:100])
                        time.sleep(2)
                except Exception as e3:
                    log(f'  {label}：分類の一覧も読めないので、直接の分類だけで探す', str(e3)[:100])
                    bs = wd_query(qid, deep=False)
        except Exception as e:
            log(f'  {label}：読めない', str(e)[:200]); continue
        n = 0
        for b in bs:
            item = b['item']['value'].rsplit('/', 1)[-1]
            if item in seen:
                continue
            p = _pt(b.get('coord', {}).get('value'))
            name = b.get('ja', {}).get('value', '').strip()
            if not p or not name or not (122 <= p[0] <= 154.5 and 20 <= p[1] <= 46):
                continue
            seen.add(item)
            en = b.get('en', {}).get('value') or None
            adm = b.get('adminJa', {}).get('value') or None
            # 知名度：Wikipedia に記事がある言語の数（Nominatim と同じ考え方）。80以上で1.0
            try:
                sl = int(b.get('sl', {}).get('value') or 0)
            except ValueError:
                sl = 0
            pop = min(1.0, math.log1p(sl) / math.log1p(80))
            rows.append((f'wd:{item}', 'JP', cat, name, en, adm, p[1], p[0], 'Wikidata', pop, None))
            n += 1
        log(f'  Wikidata {label}：{n} 件')
        info[-1] += f' → {n} 件'
        time.sleep(2)
    # 11番目：別名（Wikidata の日本語の別名。名前と同じように探せる）
    try:
        alts = wd_alt_labels([r[0][3:] for r in rows])
        rows = [r[:10] + (_alt_join(r[3], alts.get(r[0][3:], [])),) for r in rows]
        n = sum(1 for r in rows if r[10])
        log(f'  Wikidata 別名：{n} か所')
        info.append(f'別名あり: {n} か所')
    except Exception as e:
        log('  Wikidata 別名で失敗、別名なしで続ける', str(e)[:200])
        info.append(f'別名: 失敗 {str(e)[:120]}')
    return rows, info


def _nkey(name):
    s = unicodedata.normalize('NFKC', name or '').lower()
    s = re.sub(r'^道の駅', '', s.strip())   # 「道の駅 川場田園プラザ」と「川場田園プラザ」を同じとみなす
    return re.sub(r'[\s\-‐‑‒–—―−・·.,\'’`&/()「」『』【】!?〈〉<>]', '', s)


def drop_near_dupes(base, extra, meters=300):
    """extra のうち、base に同じ名前で meters 以内のものがあれば捨てる。
    捨てた分の知名度（pop）と別名（alt）は、重なった base の方に移す
    （戻り値の boosts：base の id → (pop, 別名)）"""
    grid = {}
    for r in base:
        grid.setdefault((_nkey(r[3]), round(r[6], 2), round(r[7], 2)), []).append(r)
    out, dropped, boosts = [], 0, {}
    for r in extra:
        k, la, ln = _nkey(r[3]), round(r[6], 2), round(r[7], 2)
        dup = None
        for dla in (-0.01, 0, 0.01):
            for dln in (-0.01, 0, 0.01):
                for o in grid.get((k, round(la + dla, 2), round(ln + dln, 2)), []):
                    dy = (o[6] - r[6]) * 111000
                    dx = (o[7] - r[7]) * 111000 * 0.8
                    if dx * dx + dy * dy < meters * meters:
                        dup = o; break
                if dup: break
            if dup: break
        if dup is not None:
            dropped += 1
            p0, a0 = boosts.get(dup[0], (0.0, None))
            pop = max(p0, r[9] if len(r) > 9 and r[9] else 0.0)
            alt = ' '.join(x for x in (a0, r[10] if len(r) > 10 else None) if x) or None
            if pop or alt:
                boosts[dup[0]] = (pop, alt)
        else:
            out.append(r)
    return out, dropped, boosts


def _boost(r, pop, alt):
    """知名度は大きい方、別名は足す（名前と同じものは外す）"""
    old = r[10] if len(r) > 10 else None
    alts = ((old or '') + ' ' + (alt or '')).split()
    return r[:9] + (max(r[9] if len(r) > 9 and r[9] else 0.0, pop), _alt_join(r[3], alts))


def fix_cols(rows):
    """全部の行を11列にそろえる（Supabase に送る時、行ごとに列がちがうとエラーになる）"""
    out = []
    for r in rows:
        r = tuple(r[:11]) + (None,) * (11 - len(r))
        if r[9] is None:
            r = r[:9] + (0.0,) + r[10:]
        out.append(r)
    return out


def add_jp_extras(rows, stats):
    """日本だけ：IC・病院・空港・Wikidata を足す（どれかが失敗しても、ほかは続ける）"""
    for label, fn in (('IC・JCT（N06）', n06_rows), ('病院（P04）', p04_rows), ('空港（C28）', c28_rows)):
        try:
            extra, url = fn()
            rows = rows + extra
            stats.append(f'JP {label}: {len(extra)} 件 {url or "（読めない）"}')
        except Exception as e:
            log(f'{label} で失敗、足さずに続ける:', str(e)[:300])
            stats.append(f'JP {label}: 失敗 {str(e)[:120]}')
    try:
        wd, info = wikidata_rows()
        wd, dropped, boosts = drop_near_dupes(rows, wd)
        if boosts:
            rows = [_boost(r, *boosts[r[0]]) if r[0] in boosts else r for r in rows]
        rows = rows + wd
        stats.append(f'JP Wikidata: {len(wd)} 件（Overture などと重なる {dropped} 件は捨て、知名度だけ {len(boosts)} 件に移した）')
        stats.extend('  ' + i for i in info)
    except Exception as e:
        log('Wikidata で失敗、足さずに続ける:', str(e)[:300])
        stats.append(f'JP Wikidata: 失敗 {str(e)[:120]}')
    return rows


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
    """置き場所（places_stage）に入れてから、2万件ずつ本番（places）に移し、最後に古いデータを消す
    （1回の処理を短くして、Supabase の時間切れで止まらないようにする。途中で失敗しても本番は空にならない）"""
    cols = ['id', 'country', 'cat', 'name', 'name_en', 'addr', 'lat', 'lng', 'src', 'pop', 'alt']
    # 前回の残りを空にする
    while True:
        n = sb_request('POST', '/rest/v1/rpc/places_stage_clear_chunk', {'p_country': country, 'p_n': 20000})
        if not n:
            break
        log(f'  置き場所の前回の残りを消した {n} 件')
    for i in range(0, len(rows), BATCH):
        chunk = [dict(zip(cols, r)) for r in rows[i:i + BATCH]]
        sb_request('POST', '/rest/v1/places_stage', chunk, prefer='return=minimal')
        if (i // BATCH) % 10 == 0 or i + BATCH >= len(rows):
            log(f'  入れた {min(i + BATCH, len(rows))}/{len(rows)}')
    min_rows = max(100, int(len(rows) * 0.5))
    sb_request('POST', '/rest/v1/rpc/places_check', {'p_country': country, 'p_min_rows': min_rows})
    run = time.strftime('%Y%m%d%H%M%S') + f'-{os.getpid()}'
    moved = 0
    while True:
        n = sb_request('POST', '/rest/v1/rpc/places_move_chunk', {'p_country': country, 'p_run': run, 'p_n': 20000})
        if n == 0:
            break
        moved += max(n, 0)
        log(f'  本番へ移した {moved} 件')
    cleared = 0
    while True:
        n = sb_request('POST', '/rest/v1/rpc/places_clear_chunk', {'p_country': country, 'p_run': run, 'p_n': 20000})
        if not n:
            break
        cleared += n
        log(f'  古いデータを消した {cleared} 件')
    n = sb_request('POST', '/rest/v1/rpc/places_finalize', {'p_country': country})
    log(f'{country}: places を入れ替えた（新しく {moved} 件、古い {cleared} 件を消した、同じ名前の数を {n} 件更新）')


# ---------------------------------------------------------------------
# 数えるだけ：飲食店（PLACES_MODE=count_food）
# ---------------------------------------------------------------------
FOOD_WORDS = ['eat_and_drink', 'restaurant', 'cafe', 'coffee', 'fast_food', 'bakery', 'izakaya', 'ramen',
              'sushi', 'noodle', 'udon', 'soba', 'diner', 'dessert', 'ice_cream', 'donut', 'burger', 'pizza',
              'yakiniku', 'curry', 'tea_room', 'bistro', 'gyudon', 'family_style', 'buffet']


def food_count(con, path, country):
    """飲食店の数を、種類別・チェーン名別に数える（データベースには入れない）"""
    xmin, ymin, xmax, ymax = COUNTRY_BBOX[country]
    cc = f"country = '{country}'"
    if country == 'JP':
        cc += " OR (country IS NULL AND regexp_matches(name, '[ぁ-んァ-ヶ一-龯]'))"
    food = ' OR '.join(f"strpos(k, '{w}') > 0" for w in FOOD_WORDS)
    tries = [
        ("lower(concat_ws(',', basic_category, taxonomy.primary, array_to_string(taxonomy.hierarchy, ','), "
         "array_to_string(taxonomy.alternates, ',')))", 'taxonomy.primary', "brand.names.primary",
         "AND coalesce(operating_status, '') <> 'permanently_closed'"),
        ("lower(concat_ws(',', basic_category, taxonomy.primary, array_to_string(taxonomy.hierarchy, ','), "
         "array_to_string(taxonomy.alternates, ',')))", 'taxonomy.primary', 'NULL',
         "AND coalesce(operating_status, '') <> 'permanently_closed'"),
        ("lower(concat_ws(',', categories.primary, array_to_string(categories.alternate, ',')))",
         'categories.primary', 'NULL', ''),
    ]
    last = None
    for k, prim, brand, closed in tries:
        try:
            t0 = time.time()
            con.execute('DROP TABLE IF EXISTS food')
            con.execute(f"""
            CREATE TEMP TABLE food AS
            WITH src AS (
              SELECT names.primary AS name, addresses[1].country AS country, {prim} AS prim,
                     {brand} AS brand, {k} AS k
              FROM read_parquet('{path}')
              WHERE bbox.xmin >= {xmin} AND bbox.xmax <= {xmax} AND bbox.ymin >= {ymin} AND bbox.ymax <= {ymax}
                AND coalesce(confidence, 1) >= {MIN_CONF} {closed}
            )
            SELECT name, prim, brand,
                   split_part(trim(replace(name, '　', ' ')), ' ', 1) AS head FROM src
            WHERE ({cc}) AND name IS NOT NULL AND name <> '' AND ({food})
            """)
            log(f'{country}: 飲食店を取り出した（{time.time() - t0:.0f}秒）')
            break
        except duckdb.Error as e:
            last = e
            log('この形では読めない →', str(e).splitlines()[0][:200])
    else:
        raise SystemExit(f'取り出し失敗: {last}')

    q = lambda sql: con.execute(sql).fetchall()
    out = []
    total = q('SELECT count(*) FROM food')[0][0]
    out.append(f'[{country}] 飲食店 合計 {total} 件')
    out.append('')
    out.append('■ 種類別（多い順 40）')
    for p, n in q('SELECT coalesce(prim, \'(なし)\'), count(*) n FROM food GROUP BY 1 ORDER BY n DESC LIMIT 40'):
        out.append(f'  {n:>7}  {p}')
    out.append('')
    out.append('■ チェーン店の見当（名前の最初の言葉＝空白の前 が同じ店の数で分けた件数）')
    for th in (5, 10, 20, 50, 100):
        names, rows = q(f'SELECT count(*), coalesce(sum(n), 0) FROM (SELECT head, count(*) n FROM food GROUP BY head) WHERE n >= {th}')[0]
        out.append(f'  {th:>3} 店以上：{names:>5} チェーン・合計 {rows:>7} 件')
    nb = q('SELECT count(*) FROM food WHERE brand IS NOT NULL')[0][0]
    out.append('')
    out.append(f'■ ブランド名あり：{nb} 件（多い順 80）')
    for b, n in q('SELECT brand, count(*) n FROM food WHERE brand IS NOT NULL GROUP BY 1 ORDER BY n DESC LIMIT 80'):
        out.append(f'  {n:>6}  {b}')
    out.append('')
    out.append('■ 名前の最初の言葉（多い順 80）')
    for nm, n in q('SELECT head, count(*) n FROM food GROUP BY 1 ORDER BY n DESC LIMIT 80'):
        out.append(f'  {n:>6}  {nm}')
    for kw in ('スターバックス', 'スタバ', 'starbucks', 'マクドナルド', 'すき家', 'ドトール', 'コメダ'):
        n = q(f"SELECT count(*) FROM food WHERE strpos(lower(name), lower('{kw}')) > 0")[0][0]
        out.append(f'  名前に「{kw}」をふくむ：{n} 件')
    return out


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    if MODE == 'count_food':
        con = duckdb.connect()
        con.execute('INSTALL httpfs; LOAD httpfs;')
        con.execute("SET s3_region='us-west-2';")
        rel = latest_release(con)
        path = f's3://overturemaps-us-west-2/release/{rel}/theme=places/type=place/*.parquet'
        lines = [f'Overture release: {rel}', f'min confidence: {MIN_CONF}', '数えるだけ（Supabase には何もしない）', '']
        for country in COUNTRIES:
            lines += food_count(con, path, country) + ['']
        txt = '\n'.join(lines)
        log('\n' + txt)
        with open(os.path.join(OUT_DIR, 'food_count.txt'), 'w', encoding='utf-8') as f:
            f.write(txt + '\n')
        return
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
        rows = [tuple(r) + (0.0,) for r in extract(con, path, country)]   # 10番目：知名度（Overture は 0）
        if country == 'JP':
            rows, n02url = replace_jp_stations(rows)
            stats.append(f'JP stations: {"国土数値情報 " + n02url if n02url else "Overture（国土数値情報が使えなかった）"}')
            rows = add_jp_extras(rows, stats)
        rows = fix_cols(rows)
        counts = {}
        for r in rows:
            counts[r[2]] = counts.get(r[2], 0) + 1
        stats.append(f'[{country}] total {len(rows)}')
        for c, n in sorted(counts.items(), key=lambda x: -x[1]):
            stats.append(f'  {c}: {n}')
        with gzip.open(os.path.join(OUT_DIR, f'places_{country}.csv.gz'), 'wt', encoding='utf-8', newline='') as f:
            w = csv.writer(f)
            w.writerow(['id', 'country', 'cat', 'name', 'name_en', 'addr', 'lat', 'lng', 'src', 'pop', 'alt'])
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
