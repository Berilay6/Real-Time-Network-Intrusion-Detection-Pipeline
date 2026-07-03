from datafusion import SessionContext
import pandas as pd

# Sahte timestamp uret: satirlara 1 saniye araliklarla zaman ata
df = pd.read_csv("data/clean/Friday-WorkingHours-Morning.pcap_ISCX.csv")
df["injected_ts"] = pd.date_range("2026-01-01 00:00:00", periods=len(df), freq="s")
df.to_csv("data/clean/_windowing_test.csv", index=False)

ctx = SessionContext()
ctx.register_csv("traffic", "data/clean/_windowing_test.csv")

# 60 saniyelik tumbling window: her pencerede flow sayisi ve BENIGN olmayan oran
result = ctx.sql("""
    SELECT
        date_trunc('minute', injected_ts) as window_start,
        COUNT(*) as total_flows,
        SUM(CASE WHEN "Label" != 'BENIGN' THEN 1 ELSE 0 END) as attack_flows
    FROM traffic
    GROUP BY date_trunc('minute', injected_ts)
    ORDER BY window_start
    LIMIT 10
""")

result.show()
