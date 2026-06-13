import csv
import random
from datetime import datetime, timedelta

output_file = "transactions_v2.csv"

NUM_ROWS = 300_000

regions = ["DE-HE", "DE-BY", "DE-BW", "DE-NW", "DE-HH"]
campaign_types = ["credit_card_offer", "mortgage_offer", "cash_loan", "insurance_offer"]
call_statuses = ["answered", "missed", "dropped"]
client_responses = ["interested", "not_interested", "callback_later", "no_answer"]

start_dt = datetime(2026, 5, 1, 9, 0, 0)

with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    # заголовок
    writer.writerow([
        "call_id",
        "call_time",
        "client_id",
        "region_code",
        "campaign_type",
        "call_status",
        "client_response",
        "duration_sec",
        "follow_up_required"
    ])

    for i in range(1, NUM_ROWS + 1):
        call_time = start_dt + timedelta(seconds=random.randint(0, 60 * 60 * 24))
        row = [
            f"call_20260501_{i:06d}",
            call_time.strftime("%Y-%m-%d %H:%M:%S"),
            f"client_{random.randint(1000, 9999)}",
            random.choice(regions),
            random.choice(campaign_types),
            random.choice(call_statuses),
            random.choice(client_responses),
            random.randint(0, 600),
            random.choice(["true", "false"])
        ]
        writer.writerow(row)

print("Готово, файл создан:", output_file)