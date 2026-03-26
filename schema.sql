-- Echo Finance v1.0.0 — D1 Schema

CREATE TABLE IF NOT EXISTS tenants (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT,
  currency TEXT DEFAULT 'USD',
  plan TEXT DEFAULT 'starter',
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS accounts (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  name TEXT NOT NULL,
  account_type TEXT DEFAULT 'checking',
  currency TEXT DEFAULT 'USD',
  balance REAL DEFAULT 0,
  institution TEXT,
  account_number_last4 TEXT,
  is_active INTEGER DEFAULT 1,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
CREATE INDEX IF NOT EXISTS idx_acct_tenant ON accounts(tenant_id);

CREATE TABLE IF NOT EXISTS transactions (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  account_id TEXT,
  amount REAL NOT NULL,
  tx_type TEXT DEFAULT 'expense',
  category TEXT DEFAULT 'uncategorized',
  subcategory TEXT,
  description TEXT,
  payee TEXT,
  tx_date TEXT NOT NULL,
  is_recurring INTEGER DEFAULT 0,
  tags TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (tenant_id) REFERENCES tenants(id),
  FOREIGN KEY (account_id) REFERENCES accounts(id)
);
CREATE INDEX IF NOT EXISTS idx_tx_tenant ON transactions(tenant_id, tx_date);
CREATE INDEX IF NOT EXISTS idx_tx_account ON transactions(tenant_id, account_id);
CREATE INDEX IF NOT EXISTS idx_tx_category ON transactions(tenant_id, category);
CREATE INDEX IF NOT EXISTS idx_tx_type ON transactions(tenant_id, tx_type);

CREATE TABLE IF NOT EXISTS budgets (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  category TEXT NOT NULL,
  amount REAL NOT NULL,
  period_start TEXT NOT NULL,
  period_end TEXT,
  alert_threshold INTEGER DEFAULT 80,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
CREATE INDEX IF NOT EXISTS idx_budget_tenant ON budgets(tenant_id);

CREATE TABLE IF NOT EXISTS goals (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  name TEXT NOT NULL,
  description TEXT,
  target_amount REAL DEFAULT 0,
  current_amount REAL DEFAULT 0,
  target_date TEXT,
  category TEXT DEFAULT 'savings',
  priority TEXT DEFAULT 'medium',
  status TEXT DEFAULT 'active',
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
CREATE INDEX IF NOT EXISTS idx_goals_tenant ON goals(tenant_id);

CREATE TABLE IF NOT EXISTS recurring (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  name TEXT NOT NULL,
  amount REAL NOT NULL,
  frequency TEXT DEFAULT 'monthly',
  category TEXT DEFAULT 'bills',
  account_id TEXT,
  next_date TEXT,
  payee TEXT,
  is_active INTEGER DEFAULT 1,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (tenant_id) REFERENCES tenants(id),
  FOREIGN KEY (account_id) REFERENCES accounts(id)
);
CREATE INDEX IF NOT EXISTS idx_recurring_tenant ON recurring(tenant_id);
CREATE INDEX IF NOT EXISTS idx_recurring_next ON recurring(is_active, next_date);
