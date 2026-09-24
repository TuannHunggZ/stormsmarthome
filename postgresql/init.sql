CREATE TABLE IF NOT EXISTS plug_data (
    house_id      INT NOT NULL,
    household_id  INT NOT NULL,
    plug_id       INT NOT NULL,
    year          VARCHAR(4) NOT NULL,
    month         VARCHAR(2) NOT NULL,
    day           VARCHAR(2) NOT NULL,
    slice_gap     INT NOT NULL,
    slice_index   INT NOT NULL,
    value         DOUBLE PRECISION NOT NULL,
    count         DOUBLE PRECISION NOT NULL,
    avg           DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (
        house_id,
        household_id,
        plug_id,
        year,
        month,
        day,
        slice_gap,
        slice_index
    )
);

CREATE TABLE IF NOT EXISTS house_data (
    house_id    INTEGER NOT NULL,
    year        VARCHAR(4) NOT NULL,
    month       VARCHAR(2) NOT NULL,
    day         VARCHAR(2) NOT NULL,
    slice_gap   INTEGER NOT NULL,
    slice_index INTEGER NOT NULL,
    avg         DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (
        house_id,
        year,
        month,
        day,
        slice_gap,
        slice_index
    )
);

CREATE TABLE IF NOT EXISTS plug_data_forecast (
    house_id     INTEGER NOT NULL,
    household_id INTEGER NOT NULL,
    plug_id      INTEGER NOT NULL,
    year         VARCHAR(4) NOT NULL,
    month        VARCHAR(2) NOT NULL,
    day          VARCHAR(2) NOT NULL,
    slice_gap    INTEGER NOT NULL,
    slice_index  INTEGER NOT NULL,
    avg          DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (
        house_id,
        household_id,
        plug_id,
        year,
        month,
        day,
        slice_gap,
        slice_index
    )
);

CREATE TABLE IF NOT EXISTS house_data_forecast (
    house_id     INTEGER NOT NULL,
    year         VARCHAR(4) NOT NULL,
    month        VARCHAR(2) NOT NULL,
    day          VARCHAR(2) NOT NULL,
    slice_gap    INTEGER NOT NULL,
    slice_index  INTEGER NOT NULL,
    avg          DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (
        house_id,
        year,
        month,
        day,
        slice_gap,
        slice_index
    )
);