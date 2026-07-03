from datafusion import SessionContext

ctx = SessionContext()
ctx.register_csv("traffic", "data/clean/Friday-WorkingHours-Morning.pcap_ISCX.csv")

df = ctx.sql("""
    SELECT "Destination Port", COUNT(*) as flow_count
    FROM traffic
    GROUP BY "Destination Port"
    ORDER BY flow_count DESC
    LIMIT 10
""")

df.show()
