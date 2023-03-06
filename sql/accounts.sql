create table accounts (
    id mediumint not null auto_increment,
    username text not null,
    password text,
    last_use bigint default 0,
    in_use_by text,
    last_returned bigint default 0,
    primary key (username))
