using Binance.Net;
using Binance.Net.Objects;
using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Globalization;
using System.Threading;

namespace KafkaSocketTest.SocketListener
{
    public class SocketListener
    {
        BinanceSocketClient _socketClient;
        BinanceClient _apiClient;
        Producer<Null, string> _producer = new Producer<Null, string>(new ProducerConfig { BootstrapServers = "localhost:9092" });
        Action<DeliveryReportResult<Null, string>> handler = r =>
         Console.WriteLine(!r.Error.IsError
             ? $"Delivered message to {r.TopicPartitionOffset}"
             : $"Delivery Error: {r.Error.Reason}");
        public SocketListener(string key = "i8esSKM1QA6H7DgyCLPFDf17bPXRsWu7ZxBoD4uothQXwRGpvFkJeuOgRgp7cYNM", string secret = "iKWu94GjIwVLBoPORH0pt1sUuqf3MtJobGrQjdu4033kje43Nhe8tQnSK9pnXXgH", string name = "Test", long channelId = -1001483025408)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
            _apiClient = new BinanceClient(new BinanceClientOptions() { ApiCredentials = new CryptoExchange.Net.Authentication.ApiCredentials(key, secret) });
            _socketClient = new BinanceSocketClient(new BinanceSocketClientOptions() { ApiCredentials = new CryptoExchange.Net.Authentication.ApiCredentials(key, secret) });
        }

        public void Subcribe(string pair)
        {
            var symbolTtradeSubscription = _socketClient.SubscribeToTradesStream(pair, _ => ProduceEvent(_));
            if (symbolTtradeSubscription.Success)
                Console.WriteLine(symbolTtradeSubscription.Data.Id);
            else
                Console.WriteLine(symbolTtradeSubscription.Error.Message);
        }       
        public void ProduceEvent(BinanceStreamTrade e)
        {
            try
            {
                Console.WriteLine("catched");
                _producer.BeginProduce("socket", new Message<Null, string>
                {
                    Value = JsonConvert.SerializeObject(e)
                }, handler);
                _producer.Flush(TimeSpan.FromSeconds(10));
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());                
            }
        }







    }
}
