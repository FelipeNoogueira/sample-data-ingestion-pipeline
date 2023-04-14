create schema if not exists weather;

create table if not exists weather.hourly(
    location varchar(40) not null,
    time timestamp without time zone not null,
    temp_celsius numeric(5, 2) not null,
    condition varchar(200) not null,
    primary key (location, time)
);