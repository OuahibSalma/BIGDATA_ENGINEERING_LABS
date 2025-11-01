create database hotelbooking;

use hotelbooking;

create table clients(clientid int, nom string, email string, telephone string)
row format delimited fields terminated by ','
stored as textfile;

create table hotels(hotelid int, nom string, villes string, etoiles int)
row format delimited fields terminated by ','
stored as textfile;

create table reservations(reservationid int, clientid int, hotelid int, datedebut string, datefin string, prixtotal float)
row format delimited fields terminated by ','
stored as textfile;