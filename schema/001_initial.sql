create table if not exists trade_intents (
  intent_id text primary key,
  created_at text not null,
  symbol text not null,
  side text not null check (side in ('long', 'short', 'flat')),
  action text not null check (action in ('open', 'close', 'reduce', 'reverse')),
  target_notional real not null,
  max_order_notional real not null,
  status text not null check (status in ('pending', 'accepted', 'rejected', 'executed', 'failed')),
  source text not null,
  reason text,
  model_version text,
  processed_at text,
  error text
);

create table if not exists control_commands (
  command_id text primary key,
  created_at text not null,
  command text not null check (command in ('stop', 'resume', 'close_all', 'cancel_all')),
  status text not null check (status in ('pending', 'accepted', 'rejected', 'executed', 'failed')),
  requested_by text not null,
  processed_at text,
  error text
);

create table if not exists orders (
  order_id text primary key,
  client_oid text not null unique,
  intent_id text,
  symbol text not null,
  side text not null,
  action text not null,
  order_type text not null,
  status text not null,
  price real,
  size real,
  filled_size real not null default 0,
  created_at text not null,
  updated_at text not null,
  foreign key (intent_id) references trade_intents(intent_id)
);

create table if not exists fills (
  fill_id text primary key,
  order_id text not null,
  symbol text not null,
  side text not null,
  price real not null,
  size real not null,
  fee real not null,
  created_at text not null,
  foreign key (order_id) references orders(order_id)
);

create table if not exists positions (
  symbol text primary key,
  side text not null,
  notional real not null,
  entry_price real not null,
  unrealized_pnl real not null,
  updated_at text not null
);

create table if not exists equity_snapshots (
  snapshot_id text primary key,
  created_at text not null,
  equity real not null,
  available_margin real not null,
  unrealized_pnl real not null,
  realized_pnl_24h real not null
);

create table if not exists models (
  model_version text primary key,
  created_at text not null,
  train_start text not null,
  train_end text not null,
  validation_start text not null,
  validation_end text not null,
  artifact_path text not null,
  artifact_hash text not null,
  metrics_json text not null
);

create table if not exists events (
  event_id text primary key,
  created_at text not null,
  severity text not null check (severity in ('info', 'warning', 'error', 'critical')),
  component text not null,
  message text not null,
  payload_json text not null default '{}',
  delivered_to_telegram integer not null default 0
);

create table if not exists task_checkpoints (
  task_name text primary key,
  updated_at text not null,
  checkpoint_value text not null
);

create index if not exists idx_trade_intents_status_created
  on trade_intents(status, created_at);

create index if not exists idx_control_commands_status_created
  on control_commands(status, created_at);

create index if not exists idx_events_delivery
  on events(delivered_to_telegram, severity, created_at);
