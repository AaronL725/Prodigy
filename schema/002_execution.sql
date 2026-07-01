alter table orders add column exchange_order_id text;
alter table orders add column attempt integer not null default 1;
alter table orders add column raw_json text not null default '{}';
alter table orders add column last_error text;

alter table fills add column trade_id text;
alter table fills add column client_oid text;
alter table fills add column raw_json text not null default '{}';

alter table positions add column ownership text not null default 'system'
  check (ownership in ('system', 'imported'));
alter table positions add column opened_at text;
alter table positions add column adopted_at text;
alter table positions add column source_intent_id text;
alter table positions add column raw_json text not null default '{}';

create table if not exists executor_state (
  key text primary key,
  value text not null,
  updated_at text not null
);

create index if not exists idx_orders_intent_status
  on orders(intent_id, status, updated_at);

create index if not exists idx_fills_order_symbol
  on fills(order_id, symbol, created_at);
