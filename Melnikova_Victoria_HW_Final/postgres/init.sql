CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(50) PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS user_sessions_raw (
    session_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES users(user_id),
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    pages_visited TEXT[],
    device VARCHAR(20),
    actions TEXT[],
    duration DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES users(user_id),
    status VARCHAR(20),
    issue_type VARCHAR(20),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    resolution_time_hours DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_sessions_user_time ON user_sessions_raw (user_id, start_time);
CREATE INDEX IF NOT EXISTS idx_tickets_status_type ON support_tickets (status, issue_type);
