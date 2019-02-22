using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace srds_cassandra.Backend
{
    public class Cli
    {
        BackendSession backend;

        public Cli()
        {
            backend = new BackendSession("127.0.0.1", "booker");
        }

        public void Run()
        {
            bool runnable = true;
            while (runnable)
            {
                String fullCommand = Console.ReadLine();
                // Console.WriteLine(fullCommand.Split(null)[0]);
                try
                {
                    switch (fullCommand.Split(null)[0])
                    {
                        case "book":
                            book();
                            break;
                        case "unbook":
                            unbook();
                            break;
                        case "addHotel":
                            addHotel();
                            break;
                        case "stress":
                            stressTest();
                            break;
                        case "help":
                            showHelp();
                            break;
                        case "clr":
                            deleteAll();
                            break;
                        case "exit":
                            Console.WriteLine("exitting");
                            deleteAll();
                            runnable = false;
                            break;
                        default:
                            Console.WriteLine("Command unrecognized");
                            showHelp();
                            break;
                    }
                }
                catch (System.Exception)
                {
                    
                    throw;
                }
            }
        }

        private void showHelp()
        {
            Console.WriteLine("-> HELP <-");
            Console.WriteLine("\tbook\t\tbook rooms in hotel");
            Console.WriteLine("\tunbook\t\tunbook rooms");
            Console.WriteLine("\taddHotel\tadd hotels to database");
            Console.WriteLine("\tstress\t\tperform high load stress test");
            Console.WriteLine("\n\texit");
        }

        private void book()
        {
            List<int> hotels = backend.getAllHotels();
            Console.WriteLine("Hotels available: (hotelIds)");
            foreach (var hotel in hotels)
            {
                Console.WriteLine("{0}", hotel);
            }
            Console.Write("select one hotelId: ");
            string hotelIdStr = Console.ReadLine();
            Console.Write("insert user name: ");
            string client = Console.ReadLine();
            Console.Write("insert how many rooms you want to book: ");
            string countStr = Console.ReadLine();

            bool result = backend.bookRooms(Int32.Parse(hotelIdStr), client, Int32.Parse(countStr));
            if (result)
            {
                Console.WriteLine("Successfully booked: {0} room(s) at: {1}",
                    countStr, hotelIdStr);
            }
            else
            {
                Console.WriteLine("Couldn't book - not enough free rooms");
            }
        }

        private void unbook()
        {
            
        }

        private void addHotel()
        {
            Console.Write("insert hotelId: ");
            string hotelIdStr = Console.ReadLine();
            Console.Write("insert hotel name: ");
            string name = Console.ReadLine();
            Console.Write("insert how many rooms hotel has: ");
            string roomsCountStr = Console.ReadLine();

            backend.addHotel(Int32.Parse(hotelIdStr), name, Int32.Parse(roomsCountStr));
            Console.WriteLine("Successfully added hotel: {0} {1} with: {2} rooms",
                hotelIdStr, name, roomsCountStr);
        }

        private string generateUser()
        {
            Random random = new Random();
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

            return new string(Enumerable.Repeat(chars, 8)
                .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        private void runClient(int offset, int roomCount)
        {
            // Console.WriteLine("Reserving from {0} to {1}", offset, offset + roomCount -1);
            String client = generateUser();
            int booked = 0;
            for (int i= 0; i < roomCount; i++)
            {
                
                if (backend.bookSpecificRoom(90210, client, offset + i))
                {
                    booked += 1;
                }
                else
                {
                    Console.WriteLine("Couldn't book hotel");
                }
            }

            List<int> bookedRooms = backend.getBookedRooms(90210, client);
            if (bookedRooms.Count < booked)
            {
                Console.WriteLine("Client {0} booked: {1} but in system: {2}",
                    client, booked, bookedRooms.Count);
            }
        }

        private void createStressEnv()
        {
            backend.addHotel(90210, "stressHotel", 3000);
        }

        public void deleteAll()
        {
            backend.deleteAll();
        }

        public void stressTest()
        {
            int offset = 0;
            // int roomCount = 0;
            try
            {
                createStressEnv();
                
                var watch = System.Diagnostics.Stopwatch.StartNew();
                List<Thread> threads = new List<Thread>();
                int threadsCount = 1000;
                for (int i = 0; i < threadsCount; i++)
                {
                    var offset_ = offset;
                    var roomCount = new Random().Next(5) + 1;
                    
                    Thread t = new Thread(() => {
                        
                        try
                        {
                            runClient(offset_, roomCount);
                        }
                        catch (System.Exception)
                        {
                            throw;
                        }
                    });
                    offset += roomCount;
                    threads.Add(t);
                    t.Start();
                }

                foreach (Thread th in threads)
                {
                    th.Join();
                }
                watch.Stop();
                var elapsedMs = watch.ElapsedMilliseconds;
                Console.WriteLine("finnished stress test");
                Console.WriteLine("done {0} writes and {1} reads in {2}ms", offset, threadsCount, elapsedMs);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            {
                // backend.deleteAll();
            }
        }

    }
}