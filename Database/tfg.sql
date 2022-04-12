drop table if exists tfgdata2;

select * from tfgdata2;

create table tfgdata2 (
lat char(45) not null,
longi char(45) not null,
date_measure char(45) not null,
hour_measure char(45) not null,
gas_name char(45) not null,
gas_measure char(45) not null,
station_type char(45) not null,
entry_date datetime not null,
origin_measure char(45) not null,
PRIMARY KEY (lat, longi, date_measure, hour_measure, gas_name, entry_date)
);

select count(*) from tfgdata2;