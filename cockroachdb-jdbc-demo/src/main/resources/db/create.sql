drop table if exists account;

create type if not exists account_type as enum ('S', 'U');

create table if not exists account
(
    id           int            not null primary key,
    balance      numeric(19, 2) not null,
    name         varchar(128)   not null,
    type         account_type   not null,
    updated_at   timestamptz    not null default clock_timestamp()
);

alter table account
    add constraint if not exists check_account_positive_balance check (balance >= 0);

truncate table account;

insert into account (id, balance, name, type)
select i,
       100000.00,
       concat('system:', (i::varchar)),
       'S'
from generate_series(1, 10) as i;

insert into account (id, balance, name, type)
select i,
       0.00,
       concat('user:', (i::varchar)),
       'U'
from generate_series(11, 1010) as i;
