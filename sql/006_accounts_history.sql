CREATE TABLE accounts_history (
                          id mediumint NOT NULL AUTO_INCREMENT,
                          username varchar(20) not null,
                          acquired bigint default 0,
                          burned bigint default 0,
                          reason varchar(50),
                          encounters bigint default 0,
                          PRIMARY KEY (id))