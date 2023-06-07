CREATE TABLE accounts_history (
                          id mediumint NOT NULL AUTO_INCREMENT,
                          username varchar(20) not null,
                          device varchar(30),
                          acquired datetime,
                          returned datetime,
                          reason varchar(50),
                          encounters bigint default 0,
                          PRIMARY KEY (id))