using System;
using System.Collections.Generic;
using System.ComponentModel;
using Cassandra;
using Cassandra.Mapping;


namespace srds_cassandra.Backend
{
    public class BackendSession
    {
        private static PreparedStatement SELECT_FROM_FREEROOMS_BY_HOTEL;
	    private static PreparedStatement INSERT_INTO_FREEROOMS;
	    private static PreparedStatement DELETE_FROM_FREEROOMS;

	    private static PreparedStatement INSERT_INTO_BOOKEDROOMS;
	    private static PreparedStatement SELECT_FROM_BOOKEDROOMS_BY_HOTEL;
	    private static PreparedStatement SELECT_FROM_BOOKEDROOMS_BY_CLIENT;
	    private static PreparedStatement DELETE_FROM_BOOKEDROOMS;

        private static PreparedStatement SELECT_FROM_HOTELS;
        private static PreparedStatement INSERT_INTO_HOTELS;

	    private static PreparedStatement DELETE_ALL_FREEROOMS;
	    private static PreparedStatement DELETE_ALL_HOTELS;
	    private static PreparedStatement DELETE_ALL_BOOKEDROOMS;
        
        
        private ISession session;

        public BackendSession(String contactPoint, String keyspace)
        {
            Cluster cluster = Cluster.Builder()
                .AddContactPoint(contactPoint)
                .WithQueryOptions(new QueryOptions().SetConsistencyLevel(ConsistencyLevel.One))
                .Build();
            try
            {
                session = cluster.Connect(keyspace);
            }
            catch (Exception e)
            {
                throw e;
            }

            prepareStatements();
        }

        private void prepareStatements()
        {
            try
            {
                SELECT_FROM_FREEROOMS_BY_HOTEL = session.Prepare(
                    "SELECT * FROM freeRooms WHERE hotelId = ? ;"
                );
                INSERT_INTO_FREEROOMS = session.Prepare(
                    "INSERT INTO freeRooms (hotelId, roomId) VALUES (?, ?);"
                );
                DELETE_FROM_FREEROOMS = session.Prepare(
                    "DELETE FROM freeRooms WHERE hotelId = ? AND roomId = ? ;"
                );
                
                
                INSERT_INTO_BOOKEDROOMS = session.Prepare(
                    "INSERT INTO bookedRooms (hotelId, roomId, client) VALUES (?, ?, ?);"
                );
                SELECT_FROM_BOOKEDROOMS_BY_HOTEL = session.Prepare(
                    "SELECT * FROM bookedRooms WHERE hotelId = ? ;"
                );
                SELECT_FROM_BOOKEDROOMS_BY_CLIENT = session.Prepare(
                    "SELECT * FROM bookedRooms WHERE hotelId = ? AND client = ? ;"
                );
                DELETE_FROM_BOOKEDROOMS = session.Prepare(
                    "DELETE FROM bookedRooms WHERE hotelId = ? AND client = ? AND roomId = ? ;"
                );
                
                SELECT_FROM_HOTELS = session.Prepare(
                    "SELECT * FROM hotels;"
                );
                INSERT_INTO_HOTELS = session.Prepare(
                    "INSERT INTO hotels (id, name) VALUES (?, ?);"
                );

                DELETE_ALL_FREEROOMS = session.Prepare(
                    "TRUNCATE freeRooms;"
                );
                DELETE_ALL_HOTELS = session.Prepare(
                    "TRUNCATE hotels;"
                );
                DELETE_ALL_BOOKEDROOMS = session.Prepare(
                    "TRUNCATE bookedRooms;"
                );
            }
            catch (Exception e)
            {
                throw e;
            }
        }


        // Access methods CRUD & stuff...


        public Hotel GetHotel(int hotelId)
        {
            Hotel hotel = new Hotel();
            BoundStatement bs = new BoundStatement(SELECT_FROM_FREEROOMS_BY_HOTEL);

            RowSet result;
            try
            {
                result = session.Execute(bs);
            }
            catch (Exception e)
            {
                throw e;
            }

            foreach (Row row in result)
            {
                int rRoom = (int)row.GetValue(typeof(int), "roomid");
                hotel.getFreeRooms().Add(rRoom);
            }

            bs = SELECT_FROM_BOOKEDROOMS_BY_HOTEL.Bind(hotelId);
            try
            {
                result = session.Execute(bs);
            }
            catch (Exception e)
            {
                throw e;
            }

            foreach (Row row in result)
            {
                int rRoom = (int)row.GetValue(typeof(int), "roomid");
                string rClient = (string)row.GetValue(typeof(string), "client");
                hotel.getBookedRooms().Add(rRoom, rClient);
            }
            
            return hotel;
        }


        public bool bookRooms(int hotelId, string client, int count)
        {
            BoundStatement bs = SELECT_FROM_FREEROOMS_BY_HOTEL.Bind(hotelId);
            RowSet result;

            try
            {
                result = session.Execute(bs);   
            }
            catch (Exception e)
            {
                throw e;
            }

            Hotel hotel = new Hotel();
            foreach (Row row in result)
            {
                int rRoom = (int)row.GetValue(typeof(int), "roomid");
                hotel.getFreeRooms().Add(rRoom);
            }

            if (hotel.getFreeRooms().Count < count)
                return false;
            
            int processed = 0;
            foreach (var room in hotel.getFreeRooms())
            {
                if (processed >= count)
                    break;
                
                bs = DELETE_FROM_FREEROOMS.Bind(hotelId, room);
                try
                {
                    session.Execute(bs);
                }
                catch (Exception e)
                {
                    throw e;
                }

                bs = INSERT_INTO_BOOKEDROOMS.Bind(hotelId, room, client);
                try
                {
                    session.Execute(bs);
                }
                catch (Exception e)
                {
                    throw e;
                }

                processed++;
            }

            return true;
        }


        public bool unBookRooms(int hotelId, string client)
        {
            BoundStatement bs = SELECT_FROM_BOOKEDROOMS_BY_CLIENT.Bind(hotelId, client);
            RowSet result;
            try
            {
                result = session.Execute(bs);
            }
            catch (Exception)
            {
                throw;
            }

            List<int> rooms = new List<int>();
            foreach (Row row in result)
            {
                rooms.Add((int)row.GetValue(typeof(int), "roomid"));
            }

            foreach (var rRoom in rooms)
            {
                bs = DELETE_FROM_BOOKEDROOMS.Bind(hotelId, client, rRoom);
                try
                {
                    session.Execute(bs);
                }
                catch (Exception)
                {
                    throw;
                }

                bs = INSERT_INTO_FREEROOMS.Bind(hotelId, rRoom);
                try
                {
                    session.Execute(bs);
                }
                catch (Exception)
                {
                    throw;
                }
            }

            return true;
        }


        public void addHotel(int hotelId, string name, int rooms)
        {
            BoundStatement bs = INSERT_INTO_HOTELS.Bind(hotelId, name);
            try
            {
                session.Execute(bs);
            }
            catch (Exception)
            {
                throw;
            }

            for (int i = 0; i < rooms; i++)
            {
                bs = INSERT_INTO_FREEROOMS.Bind(hotelId, i + 1);
                try
                {
                    session.Execute(bs);
                }
                catch (Exception)
                {
                    throw;
                }
            }
        }

        public List<int> getBookedRooms(int hotelId, string client)
        {
		    BoundStatement bs = SELECT_FROM_BOOKEDROOMS_BY_CLIENT.Bind(hotelId, client);
		    
		    RowSet rs;
            List<int> result = new List<int>();
            try {
                rs = session.Execute(bs);
            } catch(Exception e) {
                throw e;
            }
            
            foreach (Row row in rs)
            {
                result.Add((int)row.GetValue(typeof(int), "roomid"));
            }
            
            return result;
	    }

	public List<int> getAllHotels()
    {
        BoundStatement bs = new BoundStatement(SELECT_FROM_HOTELS);
        RowSet rs = null;
        
        List<int> result = new List<int>();
        try {
            rs = session.Execute(bs);
        } catch(Exception e) {
            throw e;
        }

        foreach (Row row in rs)
        {
            result.Add((int)row.GetValue(typeof(int), "id"));
        }

        return result;
    }
    
    public void deleteAll() {
		BoundStatement bs = new BoundStatement(DELETE_ALL_FREEROOMS);
		RowSet rs = null;
		
        try {
            rs = session.Execute(bs);
        } catch(Exception e) {
            throw e;
        }

		bs = new BoundStatement(DELETE_ALL_HOTELS);
		try {
            rs = session.Execute(bs);
        } catch(Exception e) {
            throw e;
        }

		bs = new BoundStatement(DELETE_ALL_BOOKEDROOMS);
		try {
            rs = session.Execute(bs);
        } catch(Exception e) {
            throw e;
        }
	}

	private void finalize() {
		try {
			if (session != null) {
				session.Cluster.Shutdown();
			}
		} catch (Exception e) {
			Console.WriteLine("Could not close existing cluster");
		}
	}
    }
}