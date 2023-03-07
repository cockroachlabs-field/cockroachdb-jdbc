-- drop table if exists account;
create table if not exists account
(
    id           int            not null primary key,
    balance      numeric(19, 2) not null,
    name         varchar(128)   not null,
    account_type varchar(2)     not null,
    updated      timestamptz    not null default clock_timestamp()
);

alter table account
    add constraint if not exists check_account_positive_balance check (balance >= 0);

truncate table account;

insert into account (id, balance, name, account_type)
select i,
       1000000.00,
       concat('system:', (i::varchar)),
       'S'
from generate_series(1, 1) as i;

insert into account (id, balance, name, account_type)
select i,
       0.00,
       concat('user:', (i::varchar)),
       'U'
from generate_series(2, 1000) as i;
