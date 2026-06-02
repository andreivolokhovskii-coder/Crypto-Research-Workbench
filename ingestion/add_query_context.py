"""Add query_context to all Superset charts so the dashboard can fetch data."""
import requests, json, os, sys
from dotenv import load_dotenv
load_dotenv()

BASE = 'http://superset:8088'
h_json = {'Content-Type': 'application/json'}
token = requests.post(f'{BASE}/api/v1/security/login', headers=h_json,
    json={'username':'admin','password':'admin','provider':'db'}).json()['access_token']
h = {'Authorization': f'Bearer {token}', **h_json}


def qc(ds_id, queries):
    return json.dumps({
        "datasource": {"id": ds_id, "type": "table"},
        "force": False, "queries": queries,
        "result_format": "json", "result_type": "full"
    })

def agg(groupby, metrics):
    return [{"time_range": "No filter", "granularity": None, "filters": [],
             "extras": {"having": "", "where": ""}, "applied_time_extras": {},
             "columns": groupby,
             "metrics": [{"expressionType":"SQL","sqlExpression":m[0],"label":m[1],"optionName":f"m{i}"}
                         for i, m in enumerate(metrics)],
             "row_limit": 100, "series_limit": 0, "order_desc": True,
             "annotation_layers": [], "url_params": {}, "custom_params": {}, "custom_form_data": {}}]

def big(sql, label):
    return [{"time_range": "No filter", "granularity": None, "filters": [],
             "extras": {"having": "", "where": ""}, "applied_time_extras": {},
             "columns": [],
             "metrics": [{"expressionType":"SQL","sqlExpression":sql,"label":label,"optionName":"m0"}],
             "row_limit": 1, "series_limit": 0, "order_desc": True,
             "annotation_layers": [], "url_params": {}, "custom_params": {}, "custom_form_data": {}}]

def raw(cols):
    return [{"time_range": "No filter", "granularity": None, "filters": [],
             "extras": {"having": "", "where": ""}, "applied_time_extras": {},
             "columns": cols, "metrics": [], "row_limit": 100, "series_limit": 0,
             "order_desc": True, "annotation_layers": [], "url_params": {},
             "custom_params": {}, "custom_form_data": {}}]


CHARTS = {
    1:  qc(1, big("COUNT(*)",                                   "Total Candles")),
    2:  qc(1, big("COUNT(DISTINCT symbol)",                     "Symbols Tracked")),
    3:  qc(1, agg(["symbol"],          [("round(avg(price_change_pct),3)","avg_pct"),
                                        ("round(sum(quote_volume)/1e6,1)","qvol_M")])),
    4:  qc(2, agg(["symbol","interval"],[("round(max(realized_vol_7d)*100,2)","vol_7d%"),
                                         ("round(max(realized_vol_30d)*100,2)","vol_30d%"),
                                         ("round(max(avg_true_range),4)","atr")])),
    5:  qc(2, agg(["symbol"],          [("round(max(realized_vol_30d)*100,1)","vol_30d"),
                                        ("max(window_start)","as_of")])),
    6:  qc(1, agg(["symbol"],          [("COUNT(*)","candles"),
                                        ("round(avg(price_change_pct),3)","avg_pct"),
                                        ("round(sum(is_bullish)*100.0/COUNT(*),1)","pct_bull")])),
    7:  qc(1, raw(["exchange","symbol","open_time","open","high","low","close","volume","price_change_pct"])),
    8:  qc(4, raw(["symbol","name","market_cap_rank","coin_id"])),
    9:  qc(3, agg(["exchange","symbol","interval"],[("COUNT(*)","candles"),("max(open_time)","latest")])),
    10: qc(3, agg(["symbol","interval"],           [("COUNT(*)","candles"),("max(open_time)","latest")])),
}

for chart_id, query_context in CHARTS.items():
    r = requests.put(f'{BASE}/api/v1/chart/{chart_id}', headers=h,
                     json={"query_context": query_context, "query_context_generation": False})
    print(f"Chart {chart_id}: {r.status_code}")

print("query_context added to all charts")
