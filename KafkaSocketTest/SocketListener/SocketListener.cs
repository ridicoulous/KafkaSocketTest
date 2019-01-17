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

        public void SubcribeToSocket(string pair)
        {
            var symbolTickerSubscription = _socketClient.SubscribeToSymbolTicker("WAVESBTC", _ => ProduceEvent(_));
            var symbolTtradeSubscription = _socketClient.SubscribeToTradesStream("WAVESBTC", _ => ProduceEvent(_));

        }

        public void ProduceEvent(BinanceStreamTrade e)
        {            
            _producer.BeginProduce("socket", new Message<Null, string>
            {
                Value = JsonConvert.SerializeObject(e)
            }, handler);
            // wait for up to 10 seconds for any inflight messages to be delivered.
            // p.Flush(TimeSpan.FromSeconds(10));
        }
        public void ProduceEvent(BinanceStreamTick e)
        {            
            _producer.BeginProduce("socket", new Message<Null, string>
            {
                Value = JsonConvert.SerializeObject(e)
            }, handler);
            // wait for up to 10 seconds for any inflight messages to be delivered.
            // p.Flush(TimeSpan.FromSeconds(10));
        }







    }
}
