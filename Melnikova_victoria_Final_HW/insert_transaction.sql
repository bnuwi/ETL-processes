INSERT INTO `transactions_v2` (
    call_id,
    call_time,
    client_id,
    region_code,
    campaign_type,
    call_status,
    client_response,
    duration_sec,
    follow_up_required
)
VALUES
("call_20260501_000001", DATETIME("2026-05-01T11:42:15Z"), "client_4412", "DE-HE",
 "credit_card_offer", "answered", "interested", 184, true),
("call_20260501_000002", DATETIME("2026-05-01T11:45:10Z"), "client_5123", "DE-BY",
 "mortgage_offer", "missed", "no_answer", 0, false),
("call_20260501_000003", DATETIME("2026-05-01T11:48:03Z"), "client_9821", "DE-BW",
 "cash_loan", "answered", "callback_later", 95, true);