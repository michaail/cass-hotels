using System;
using System.Collections.Generic;
using System.ComponentModel;

namespace srds_cassandra
{
    public class Hotel
    {
        private List<int> freeRooms = new List<int>();
        private Dictionary<int, String> bookedRooms = new Dictionary<int, string>();

        public List<int> getFreeRooms()
        {
            return freeRooms;
        }

        public Dictionary<int, String> getBookedRooms()
        {
            return bookedRooms;
        }

        
    }
    
}