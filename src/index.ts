/**
 * Echo Finance v1.0.0 — AI-Powered Personal & Business Finance
 * Cloudflare Worker with Hono, D1, KV, service bindings
 *
 * Features: accounts, transactions, budgets, goals, recurring payments,
 * AI categorization, spending insights, reports
 */
import { Hono } from 'hono';
import { cors } from 'hono/cors';

interface Env {
  DB: D1Database;
  CACHE: KVNamespace;
  ENGINE_RUNTIME: Fetcher;
  SHARED_BRAIN: Fetcher;
  ECHO_API_KEY?: string;
}

const app = new Hono<{ Bindings: Env }>();
app.use('*', cors({ origin: '*', allowMethods: ['GET','POST','PUT','PATCH','DELETE','OPTIONS'], allowHeaders: ['Content-Type','Authorization','X-Tenant-ID','X-Echo-API-Key'] }));

const uid = () => crypto.randomUUID();
const now = () => new Date().toISOString();
const sanitize = (s: string, max = 2000) => s?.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, '').slice(0, max) ?? '';
const sanitizeBody = (o: Record<string, unknown>) => {
  const r: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(o)) r[k] = typeof v === 'string' ? sanitize(v) : v;
  return r;
};
const tid = (c: any) => c.req.header('X-Tenant-ID') || c.req.query('tenant_id') || '';
const json = (d: unknown, s = 200) => new Response(JSON.stringify(d), { status: s, headers: { 'Content-Type': 'application/json' } });
const log = (level: string, msg: string, meta: Record<string, unknown> = {}) =>
  console.log(JSON.stringify({ level, msg, service: 'echo-finance', ts: now(), ...meta }));

// Rate limit
async function rateLimit(kv: KVNamespace, key: string, limit: number, windowSec = 60): Promise<boolean> {
  const rlKey = `rl:${key}`; const nowMs = Date.now();
  const raw = await kv.get(rlKey);
  if (!raw) { await kv.put(rlKey, JSON.stringify({ c: 1, t: nowMs }), { expirationTtl: windowSec * 2 }); return false; }
  const st = JSON.parse(raw);
  const decay = Math.max(0, st.c - ((nowMs - st.t) / 1000 / windowSec) * limit);
  const count = decay + 1;
  await kv.put(rlKey, JSON.stringify({ c: count, t: nowMs }), { expirationTtl: windowSec * 2 });
  return count > limit;
}

app.use('*', async (c, next) => {
  const path = new URL(c.req.url).pathname;
  if (path === '/health' || path === '/status') return next();
  const ip = c.req.header('cf-connecting-ip') || 'unknown';
  const isWrite = ['POST','PUT','PATCH','DELETE'].includes(c.req.method);
  if (await rateLimit(c.env.CACHE, `${ip}:${isWrite ? 'w' : 'r'}`, isWrite ? 60 : 200)) return json({ error: 'Rate limited' }, 429);
  return next();
});

app.use('*', async (c, next) => {
  const method = c.req.method; const path = new URL(c.req.url).pathname;
  if (method === 'GET' || method === 'OPTIONS' || method === 'HEAD' || path === '/health' || path === '/status') return next();
  const apiKey = c.req.header('X-Echo-API-Key') || '';
  const bearer = (c.req.header('Authorization') || '').replace('Bearer ', '');
  const expected = c.env.ECHO_API_KEY;
  if (!expected || (apiKey !== expected && bearer !== expected)) return json({ error: 'Unauthorized' }, 401);
  return next();
});

// ═══════════════ HEALTH ═══════════════
app.get('/health', async (c) => {
  let dbOk = false;
  try { await c.env.DB.prepare('SELECT 1').first(); dbOk = true; } catch {}
  return json({ status: dbOk ? 'ok' : 'degraded', service: 'echo-finance', version: '1.0.0', time: now(), db: dbOk ? 'connected' : 'error' });
});

app.get('/status', async (c) => {
  const counts = await c.env.DB.prepare(`SELECT
    (SELECT COUNT(*) FROM accounts) as accounts,
    (SELECT COUNT(*) FROM transactions) as transactions,
    (SELECT COUNT(*) FROM budgets) as budgets,
    (SELECT COUNT(*) FROM goals) as goals,
    (SELECT COUNT(*) FROM recurring) as recurring_payments,
    (SELECT COUNT(*) FROM tenants) as tenants
  `).first();
  return json({ service: 'echo-finance', version: '1.0.0', time: now(), counts });
});

// ═══════════════ TENANTS ═══════════════
app.post('/tenants', async (c) => {
  const b = sanitizeBody(await c.req.json()); const id = uid();
  await c.env.DB.prepare('INSERT INTO tenants (id, name, email, currency, plan) VALUES (?, ?, ?, ?, ?)')
    .bind(id, b.name, b.email || null, b.currency || 'USD', b.plan || 'starter').run();
  return json({ id }, 201);
});

app.get('/tenants/:id', async (c) => {
  const r = await c.env.DB.prepare('SELECT * FROM tenants WHERE id=?').bind(c.req.param('id')).first();
  return r ? json(r) : json({ error: 'Not found' }, 404);
});

// ═══════════════ ACCOUNTS ═══════════════
app.get('/accounts', async (c) => {
  const r = await c.env.DB.prepare('SELECT * FROM accounts WHERE tenant_id=? ORDER BY name').bind(tid(c)).all();
  return json(r.results);
});

app.post('/accounts', async (c) => {
  const t = tid(c); const b = sanitizeBody(await c.req.json()); const id = uid();
  await c.env.DB.prepare('INSERT INTO accounts (id, tenant_id, name, account_type, currency, balance, institution, account_number_last4, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)')
    .bind(id, t, b.name, b.account_type || 'checking', b.currency || 'USD', b.balance || 0, b.institution || null, b.account_number_last4 || null).run();
  log('info', 'account_created', { tenant_id: t, account_id: id, type: b.account_type });
  return json({ id }, 201);
});

app.get('/accounts/:id', async (c) => {
  const r = await c.env.DB.prepare('SELECT * FROM accounts WHERE id=? AND tenant_id=?').bind(c.req.param('id'), tid(c)).first();
  return r ? json(r) : json({ error: 'Not found' }, 404);
});

app.put('/accounts/:id', async (c) => {
  const b = sanitizeBody(await c.req.json());
  await c.env.DB.prepare("UPDATE accounts SET name=coalesce(?,name), account_type=coalesce(?,account_type), institution=coalesce(?,institution), balance=coalesce(?,balance), is_active=coalesce(?,is_active), updated_at=datetime('now') WHERE id=? AND tenant_id=?")
    .bind(b.name || null, b.account_type || null, b.institution || null, b.balance ?? null, b.is_active ?? null, c.req.param('id'), tid(c)).run();
  return json({ updated: true });
});

// ═══════════════ TRANSACTIONS ═══════════════
app.get('/transactions', async (c) => {
  const t = tid(c);
  const accountId = c.req.query('account_id');
  const category = c.req.query('category');
  const from = c.req.query('from');
  const to = c.req.query('to');
  const txType = c.req.query('type');
  const limit = Math.min(parseInt(c.req.query('limit') || '50'), 200);
  const offset = parseInt(c.req.query('offset') || '0');

  let q = 'SELECT t.*, a.name as account_name FROM transactions t LEFT JOIN accounts a ON a.id=t.account_id WHERE t.tenant_id=?';
  const params: (string | number)[] = [t];

  if (accountId) { q += ' AND t.account_id=?'; params.push(accountId); }
  if (category) { q += ' AND t.category=?'; params.push(sanitize(category, 50)); }
  if (txType) { q += ' AND t.tx_type=?'; params.push(sanitize(txType, 20)); }
  if (from) { q += ' AND t.tx_date>=?'; params.push(sanitize(from, 20)); }
  if (to) { q += ' AND t.tx_date<=?'; params.push(sanitize(to, 20)); }
  q += ' ORDER BY t.tx_date DESC, t.created_at DESC LIMIT ? OFFSET ?';
  params.push(limit, offset);

  const r = await c.env.DB.prepare(q).bind(...params).all();
  return json({ transactions: r.results, limit, offset });
});

app.post('/transactions', async (c) => {
  const t = tid(c); const b = sanitizeBody(await c.req.json()); const id = uid();
  const amount = Number(b.amount) || 0;
  const txType = (b.tx_type as string) || (amount < 0 ? 'expense' : 'income');

  await c.env.DB.prepare('INSERT INTO transactions (id, tenant_id, account_id, amount, tx_type, category, subcategory, description, payee, tx_date, is_recurring, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')
    .bind(id, t, b.account_id || null, amount, txType, b.category || 'uncategorized', b.subcategory || null, b.description || null, b.payee || null, b.tx_date || now().split('T')[0], b.is_recurring ? 1 : 0, b.tags || null).run();

  // Update account balance
  if (b.account_id) {
    await c.env.DB.prepare("UPDATE accounts SET balance=balance+?, updated_at=datetime('now') WHERE id=? AND tenant_id=?")
      .bind(amount, b.account_id, t).run();
  }

  log('info', 'transaction_created', { tenant_id: t, tx_id: id, amount, type: txType });
  return json({ id }, 201);
});

app.get('/transactions/:id', async (c) => {
  const r = await c.env.DB.prepare('SELECT * FROM transactions WHERE id=? AND tenant_id=?').bind(c.req.param('id'), tid(c)).first();
  return r ? json(r) : json({ error: 'Not found' }, 404);
});

app.put('/transactions/:id', async (c) => {
  const b = sanitizeBody(await c.req.json());
  await c.env.DB.prepare("UPDATE transactions SET category=coalesce(?,category), subcategory=coalesce(?,subcategory), description=coalesce(?,description), payee=coalesce(?,payee), tags=coalesce(?,tags), updated_at=datetime('now') WHERE id=? AND tenant_id=?")
    .bind(b.category || null, b.subcategory || null, b.description || null, b.payee || null, b.tags || null, c.req.param('id'), tid(c)).run();
  return json({ updated: true });
});

app.delete('/transactions/:id', async (c) => {
  const t = tid(c); const txId = c.req.param('id');
  const tx = await c.env.DB.prepare('SELECT amount, account_id FROM transactions WHERE id=? AND tenant_id=?').bind(txId, t).first() as any;
  if (!tx) return json({ error: 'Not found' }, 404);
  if (tx.account_id) {
    await c.env.DB.prepare("UPDATE accounts SET balance=balance-?, updated_at=datetime('now') WHERE id=? AND tenant_id=?")
      .bind(tx.amount, tx.account_id, t).run();
  }
  await c.env.DB.prepare('DELETE FROM transactions WHERE id=? AND tenant_id=?').bind(txId, t).run();
  return json({ deleted: true });
});

// ═══════════════ BUDGETS ═══════════════
app.get('/budgets', async (c) => {
  const t = tid(c);
  const r = await c.env.DB.prepare('SELECT * FROM budgets WHERE tenant_id=? ORDER BY category').bind(t).all();

  // Calculate spent for each budget
  const enriched = [];
  for (const budget of r.results as any[]) {
    const spent = await c.env.DB.prepare("SELECT COALESCE(SUM(ABS(amount)),0) as total FROM transactions WHERE tenant_id=? AND category=? AND tx_type='expense' AND tx_date>=? AND tx_date<=?")
      .bind(t, budget.category, budget.period_start, budget.period_end).first() as any;
    enriched.push({ ...budget, spent: spent?.total || 0, remaining: budget.amount - (spent?.total || 0), percent_used: Math.round(((spent?.total || 0) / budget.amount) * 100) });
  }
  return json(enriched);
});

app.post('/budgets', async (c) => {
  const t = tid(c); const b = sanitizeBody(await c.req.json()); const id = uid();
  await c.env.DB.prepare('INSERT INTO budgets (id, tenant_id, category, amount, period_start, period_end, alert_threshold) VALUES (?, ?, ?, ?, ?, ?, ?)')
    .bind(id, t, b.category, Number(b.amount) || 0, b.period_start || now().split('T')[0].slice(0,7) + '-01', b.period_end || null, b.alert_threshold || 80).run();
  return json({ id }, 201);
});

app.put('/budgets/:id', async (c) => {
  const b = sanitizeBody(await c.req.json());
  await c.env.DB.prepare("UPDATE budgets SET amount=coalesce(?,amount), category=coalesce(?,category), alert_threshold=coalesce(?,alert_threshold), updated_at=datetime('now') WHERE id=? AND tenant_id=?")
    .bind(b.amount ?? null, b.category || null, b.alert_threshold ?? null, c.req.param('id'), tid(c)).run();
  return json({ updated: true });
});

app.delete('/budgets/:id', async (c) => {
  await c.env.DB.prepare('DELETE FROM budgets WHERE id=? AND tenant_id=?').bind(c.req.param('id'), tid(c)).run();
  return json({ deleted: true });
});

// ═══════════════ GOALS ══════���════════
app.get('/goals', async (c) => {
  const r = await c.env.DB.prepare('SELECT * FROM goals WHERE tenant_id=? ORDER BY target_date').bind(tid(c)).all();
  const enriched = (r.results as any[]).map(g => ({
    ...g,
    progress_percent: g.target_amount > 0 ? Math.round((g.current_amount / g.target_amount) * 100) : 0,
    remaining: g.target_amount - g.current_amount
  }));
  return json(enriched);
});

app.post('/goals', async (c) => {
  const t = tid(c); const b = sanitizeBody(await c.req.json()); const id = uid();
  await c.env.DB.prepare('INSERT INTO goals (id, tenant_id, name, description, target_amount, current_amount, target_date, category, priority) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')
    .bind(id, t, b.name, b.description || null, Number(b.target_amount) || 0, Number(b.current_amount) || 0, b.target_date || null, b.category || 'savings', b.priority || 'medium').run();
  return json({ id }, 201);
});

app.put('/goals/:id', async (c) => {
  const b = sanitizeBody(await c.req.json());
  await c.env.DB.prepare("UPDATE goals SET name=coalesce(?,name), current_amount=coalesce(?,current_amount), target_amount=coalesce(?,target_amount), target_date=coalesce(?,target_date), status=coalesce(?,status), updated_at=datetime('now') WHERE id=? AND tenant_id=?")
    .bind(b.name || null, b.current_amount ?? null, b.target_amount ?? null, b.target_date || null, b.status || null, c.req.param('id'), tid(c)).run();
  return json({ updated: true });
});

// ═══════════════ RECURRING PAYMENTS ═══════════════
app.get('/recurring', async (c) => {
  const r = await c.env.DB.prepare('SELECT * FROM recurring WHERE tenant_id=? ORDER BY next_date').bind(tid(c)).all();
  return json(r.results);
});

app.post('/recurring', async (c) => {
  const t = tid(c); const b = sanitizeBody(await c.req.json()); const id = uid();
  await c.env.DB.prepare('INSERT INTO recurring (id, tenant_id, name, amount, frequency, category, account_id, next_date, payee, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)')
    .bind(id, t, b.name, Number(b.amount) || 0, b.frequency || 'monthly', b.category || 'bills', b.account_id || null, b.next_date || null, b.payee || null).run();
  return json({ id }, 201);
});

app.put('/recurring/:id', async (c) => {
  const b = sanitizeBody(await c.req.json());
  await c.env.DB.prepare("UPDATE recurring SET name=coalesce(?,name), amount=coalesce(?,amount), frequency=coalesce(?,frequency), next_date=coalesce(?,next_date), is_active=coalesce(?,is_active), updated_at=datetime('now') WHERE id=? AND tenant_id=?")
    .bind(b.name || null, b.amount ?? null, b.frequency || null, b.next_date || null, b.is_active ?? null, c.req.param('id'), tid(c)).run();
  return json({ updated: true });
});

app.delete('/recurring/:id', async (c) => {
  await c.env.DB.prepare('DELETE FROM recurring WHERE id=? AND tenant_id=?').bind(c.req.param('id'), tid(c)).run();
  return json({ deleted: true });
});

// ═══════════════ REPORTS & INSIGHTS ═══════════════
app.get('/reports/summary', async (c) => {
  const t = tid(c);
  const from = c.req.query('from') || new Date(Date.now() - 30 * 86400000).toISOString().split('T')[0];
  const to = c.req.query('to') || now().split('T')[0];

  const income = await c.env.DB.prepare("SELECT COALESCE(SUM(amount),0) as total FROM transactions WHERE tenant_id=? AND tx_type='income' AND tx_date>=? AND tx_date<=?").bind(t, from, to).first() as any;
  const expenses = await c.env.DB.prepare("SELECT COALESCE(SUM(ABS(amount)),0) as total FROM transactions WHERE tenant_id=? AND tx_type='expense' AND tx_date>=? AND tx_date<=?").bind(t, from, to).first() as any;
  const txCount = await c.env.DB.prepare("SELECT COUNT(*) as cnt FROM transactions WHERE tenant_id=? AND tx_date>=? AND tx_date<=?").bind(t, from, to).first() as any;
  const totalBalance = await c.env.DB.prepare("SELECT COALESCE(SUM(balance),0) as total FROM accounts WHERE tenant_id=? AND is_active=1").bind(t).first() as any;

  return json({
    period: { from, to },
    income: income?.total || 0,
    expenses: expenses?.total || 0,
    net: (income?.total || 0) - (expenses?.total || 0),
    transaction_count: txCount?.cnt || 0,
    total_balance: totalBalance?.total || 0,
    savings_rate: income?.total > 0 ? Math.round(((income.total - (expenses?.total || 0)) / income.total) * 100) : 0
  });
});

app.get('/reports/by-category', async (c) => {
  const t = tid(c);
  const from = c.req.query('from') || new Date(Date.now() - 30 * 86400000).toISOString().split('T')[0];
  const to = c.req.query('to') || now().split('T')[0];

  const r = await c.env.DB.prepare("SELECT category, tx_type, COALESCE(SUM(ABS(amount)),0) as total, COUNT(*) as count FROM transactions WHERE tenant_id=? AND tx_date>=? AND tx_date<=? GROUP BY category, tx_type ORDER BY total DESC")
    .bind(t, from, to).all();
  return json({ period: { from, to }, categories: r.results });
});

app.get('/reports/monthly-trend', async (c) => {
  const t = tid(c);
  const months = parseInt(c.req.query('months') || '12');
  const r = await c.env.DB.prepare(`
    SELECT strftime('%Y-%m', tx_date) as month,
      SUM(CASE WHEN tx_type='income' THEN amount ELSE 0 END) as income,
      SUM(CASE WHEN tx_type='expense' THEN ABS(amount) ELSE 0 END) as expenses,
      COUNT(*) as transactions
    FROM transactions WHERE tenant_id=? AND tx_date >= date('now', '-' || ? || ' months')
    GROUP BY month ORDER BY month
  `).bind(t, months).all();
  return json(r.results);
});

// ═══════════════ AI CATEGORIZE ═══════════════
app.post('/transactions/:id/categorize', async (c) => {
  const t = tid(c); const txId = c.req.param('id');
  const tx = await c.env.DB.prepare('SELECT * FROM transactions WHERE id=? AND tenant_id=?').bind(txId, t).first() as any;
  if (!tx) return json({ error: 'Not found' }, 404);

  try {
    const resp = await c.env.ENGINE_RUNTIME.fetch('https://engine-runtime/query', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        engine_id: 'TX01',
        query: `Categorize this financial transaction. Description: "${tx.description || 'N/A'}", Payee: "${tx.payee || 'N/A'}", Amount: $${tx.amount}. Respond as JSON: {"category":"...","subcategory":"...","confidence":0.0-1.0}. Categories: housing, utilities, food, transport, health, entertainment, shopping, education, insurance, savings, investment, income, bills, subscriptions, other`,
        max_tokens: 150
      })
    });
    const result = await resp.json() as any;
    const answer = result.response || result.answer || '';
    let cat = { category: 'other', subcategory: null as string | null, confidence: 0 };
    try { cat = JSON.parse(answer); } catch {}

    await c.env.DB.prepare("UPDATE transactions SET category=?, subcategory=?, updated_at=datetime('now') WHERE id=?")
      .bind(cat.category, cat.subcategory, txId).run();
    return json(cat);
  } catch (e: any) {
    return json({ error: 'AI categorization unavailable' }, 503);
  }
});

app.post('/insights', async (c) => {
  const t = tid(c);
  const summary = await c.env.DB.prepare("SELECT tx_type, category, SUM(ABS(amount)) as total FROM transactions WHERE tenant_id=? AND tx_date >= date('now','-30 days') GROUP BY tx_type, category ORDER BY total DESC LIMIT 20").bind(t).all();

  try {
    const resp = await c.env.ENGINE_RUNTIME.fetch('https://engine-runtime/query', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        engine_id: 'TX01',
        query: `Analyze this 30-day financial summary and provide 3-5 actionable insights. Data: ${JSON.stringify(summary.results)}. Focus on: spending patterns, savings opportunities, budget alerts, and financial health. Respond as JSON array: [{"insight":"...","type":"warning|tip|alert|positive","category":"..."}]`,
        max_tokens: 500
      })
    });
    const result = await resp.json() as any;
    const answer = result.response || result.answer || '[]';
    let insights = [];
    try { insights = JSON.parse(answer); } catch {}
    return json({ insights, generated_at: now() });
  } catch {
    return json({ insights: [{ insight: 'AI insights temporarily unavailable', type: 'alert', category: 'system' }] });
  }
});

// ═══════════════ DASHBOARD ═══════════════
app.get('/dashboard', async (c) => {
  const t = tid(c);
  const accounts = await c.env.DB.prepare('SELECT id, name, account_type, balance, currency FROM accounts WHERE tenant_id=? AND is_active=1 ORDER BY balance DESC').bind(t).all();
  const recentTx = await c.env.DB.prepare('SELECT t.id, t.amount, t.tx_type, t.category, t.description, t.payee, t.tx_date, a.name as account_name FROM transactions t LEFT JOIN accounts a ON a.id=t.account_id WHERE t.tenant_id=? ORDER BY t.tx_date DESC, t.created_at DESC LIMIT 10').bind(t).all();
  const upcomingRecurring = await c.env.DB.prepare("SELECT * FROM recurring WHERE tenant_id=? AND is_active=1 AND next_date <= date('now','+7 days') ORDER BY next_date LIMIT 5").bind(t).all();
  const goals = await c.env.DB.prepare('SELECT id, name, target_amount, current_amount, target_date FROM goals WHERE tenant_id=? AND status=? ORDER BY target_date LIMIT 5').bind(t, 'active').all();
  const totalBalance = accounts.results.reduce((sum: number, a: any) => sum + (a.balance || 0), 0);

  return json({ total_balance: totalBalance, accounts: accounts.results, recent_transactions: recentTx.results, upcoming_recurring: upcomingRecurring.results, goals: goals.results.map((g: any) => ({ ...g, progress: g.target_amount > 0 ? Math.round((g.current_amount / g.target_amount) * 100) : 0 })) });
});


app.onError((err, c) => {
  if (err.message?.includes('JSON')) {
    return c.json({ error: 'Invalid JSON body' }, 400);
  }
  console.error(`[echo-finance] ${err.message}`);
  return c.json({ error: 'Internal server error' }, 500);
});

app.notFound((c) => {
  return c.json({ error: 'Not found' }, 404);
});

export default {
  fetch: app.fetch,
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    // Process recurring payments and monthly report generation
    try {
      const due = await env.DB.prepare("SELECT * FROM recurring WHERE is_active=1 AND next_date <= date('now')").all();
      for (const r of due.results as any[]) {
        const txId = uid();
        await env.DB.prepare("INSERT INTO transactions (id, tenant_id, account_id, amount, tx_type, category, description, payee, tx_date, is_recurring) VALUES (?, ?, ?, ?, 'expense', ?, ?, ?, date('now'), 1)")
          .bind(txId, r.tenant_id, r.account_id, -Math.abs(r.amount), r.category, `Recurring: ${r.name}`, r.payee).run();

        if (r.account_id) {
          await env.DB.prepare("UPDATE accounts SET balance=balance-?, updated_at=datetime('now') WHERE id=?")
            .bind(Math.abs(r.amount), r.account_id).run();
        }

        // Advance next_date
        const freq = r.frequency;
        let nextDate = "date(next_date, '+1 month')";
        if (freq === 'weekly') nextDate = "date(next_date, '+7 days')";
        else if (freq === 'biweekly') nextDate = "date(next_date, '+14 days')";
        else if (freq === 'quarterly') nextDate = "date(next_date, '+3 months')";
        else if (freq === 'yearly') nextDate = "date(next_date, '+1 year')";
        await env.DB.prepare(`UPDATE recurring SET next_date=${nextDate}, updated_at=datetime('now') WHERE id=?`).bind(r.id).run();
      }
      log('info', 'recurring_processed', { count: due.results.length });
    } catch (e: any) {
      log('error', 'recurring_failed', { error: e.message });
    }
  }
};
