CREATE KEYSPACE IF NOT EXISTS booker
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE booker;

DROP TABLE IF EXISTS booker.freeRooms;
DROP TABLE IF EXISTS booker.bookedRooms;
DROP TABLE IF EXISTS booker.hotels;

CREATE TABLE IF NOT EXISTS freeRooms (
  hotelId int,
  roomId int,
  PRIMARY KEY (hotelId, roomId)
);

CREATE TABLE IF NOT EXISTS bookedRooms (
  hotelId int,
  client varchar,
  roomId int,
  PRIMARY KEY (hotelId, client, roomId)
);

CREATE TABLE IF NOT EXISTS hotels (
  id int,
  name varchar,
  PRIMARY KEY (id)
);