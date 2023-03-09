create table accounts (
    id mediumint not null auto_increment,
    username varchar(20) not null,
    password text,
    last_use bigint default 0,
    in_use_by text,
    last_returned bigint default 0,
    PRIMARY KEY (id),
    UNIQUE KEY (username))