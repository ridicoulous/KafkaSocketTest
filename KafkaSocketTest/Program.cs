using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaSocketTest
{
    class Program
    {
        public static void Main(string[] args)
        {
        //    Console.WriteLine("Starting...");    
            var t = new SocketListener.SocketListener();
            t.Subcribe("WAVESBTC");
            //  Consume();
            // Console.WriteLine("Consumed... PAK, pls");
            //  Console.ReadLine();
            while (true) { }

        }
        //public static void Consume()
        //{
        //    var conf = new ConsumerConfig
        //    {
        //        GroupId = "socket-consumer-group",
        //        BootstrapServers = "localhost:9092",
        //        // Note: The AutoOffsetReset property determines the start offset in the event
        //        // there are not yet any committed offsets for the consumer group for the
        //        // topic/partitions of interest. By default, offsets are committed
        //        // automatically, so in this example, consumption will only start from the
        //        // earliest message in the topic 'my-topic' the first time you run the program.
        //        AutoOffsetReset = AutoOffsetResetType.Earliest
        //    };

        //    using (var c = new Consumer<Ignore, string>(conf))
        //    {
        //        Console.WriteLine("Consumer started");
        //        c.Subscribe("socket");

        //        bool consuming = true;
        //        // The client will automatically recover from non-fatal errors. You typically
        //        // don't need to take any action unless an error is marked as fatal.
        //        c.OnError += (_, e) => consuming = !e.IsFatal;

        //        while (consuming)
        //        {
        //            //if (Console.ReadKey().Key == ConsoleKey.Escape)
        //            //    break;                        
        //            try
        //            {
        //                var cr = c.Consume();
        //                Console.WriteLine($"{cr.Value}");
        //            }
        //            catch (ConsumeException e)
        //            {
        //                Console.WriteLine($"Error occured: {e.Error.Reason}");
        //            }
        //        }
        //        // Ensure the consumer leaves the group cleanly and final offsets are committed.
        //        c.Close();
        //    }
        //}
    }
}
