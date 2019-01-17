using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaSocketTest.Messaging
{
    public class SocketProducer
    {
        ProducerConfig conf = new ProducerConfig { BootstrapServers = "localhost:9092" };
        public void Produce()
        {
            Action<DeliveryReportResult<Null, string>> handler = r =>
           Console.WriteLine(!r.Error.IsError
               ? $"Delivered message to {r.TopicPartitionOffset}"
               : $"Delivery Error: {r.Error.Reason}");
            using (var p = new Producer<Null, string>(conf))
            {
                for (int i = 0; i < 100; ++i)
                {
                    p.BeginProduce("socket", new Message<Null, string>
                    {
                        Value = i.ToString()
                    }, handler);
                }
                // wait for up to 10 seconds for any inflight messages to be delivered.
                p.Flush(TimeSpan.FromSeconds(10));
            }
        }
       
    }
}
