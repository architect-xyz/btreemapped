CREATE TABLE foobars (
    id BIGINT NOT NULL PRIMARY KEY,
    first_name TEXT,
    age INTEGER,
    is_foo BOOLEAN,
    is_bar BOOLEAN
);

CREATE PUBLICATION foobars_pub FOR TABLE foobars;