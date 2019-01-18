using Binance.Net;
using Binance.Net.Objects;
using Confluent.Kafka;
using CryptoExchange.Net.Converters;
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
        //Action<DeliveryReportResult<Null, string>> handler = r => Void(r);
        public SocketListener(string key = "i8esSKM1QA6H7DgyCLPFDf17bPXRsWu7ZxBoD4uothQXwRGpvFkJeuOgRgp7cYNM", string secret = "iKWu94GjIwVLBoPORH0pt1sUuqf3MtJobGrQjdu4033kje43Nhe8tQnSK9pnXXgH", string name = "Test", long channelId = -1001483025408)
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
            _apiClient = new BinanceClient(new BinanceClientOptions() { ApiCredentials = new CryptoExchange.Net.Authentication.ApiCredentials(key, secret) });
            _socketClient = new BinanceSocketClient(new BinanceSocketClientOptions() { ApiCredentials = new CryptoExchange.Net.Authentication.ApiCredentials(key, secret) });
        }
        public void Void(DeliveryReportResult<Null, string> r)
        {
            //if (r.Error.IsError)
            //{
            //    Console.WriteLine($"Delivery Error: {r.Error.Reason}");
            //}
        }
        public void Subcribe(string pair)
        {
            var symbolTtradeSubscription = _socketClient.SubscribeToTradesStream(pair, _ => ProduceEvent(_));
            symbolTtradeSubscription.Data.ConnectionLost += Data_ConnectionLost;

        }

        private void Data_ConnectionLost()
        {
            //Console.WriteLine("Connection lost. Reconnecting...");
            Subcribe("WAVESBTC");
        }
        public class BinanceStreamClick
        {
            public BinanceStreamClick(BinanceStreamTrade t)
            {
                Symbol = t.Symbol;
                TradeId = t.TradeId;
                Price = t.Price;
                Quantity = t.Quantity;
                BuyerOrderId = t.BuyerOrderId;
                SellerOrderId = t.SellerOrderId;
                TradeTime = t.TradeTime;
                BuyerIsMaker = t.BuyerIsMaker ? 1 : 0;
            }
            [JsonProperty("s")]
            public string Symbol { get; set; }
            [JsonProperty("t")]
            public long TradeId { get; set; }
            [JsonProperty("p")]
            public decimal Price { get; set; }
            [JsonProperty("q")]
            public decimal Quantity { get; set; }
            [JsonProperty("b")]
            public long BuyerOrderId { get; set; }
            [JsonProperty("a")]
            public long SellerOrderId { get; set; }
            [JsonConverter(typeof(TimestampConverter))]
            [JsonProperty("T")]
            public DateTime TradeTime { get; set; }
            [JsonProperty("m")]
            public int BuyerIsMaker { get; set; }           
           
        }
        public void ProduceEvent(BinanceStreamTrade e)
        {
            try
            {
                _producer.BeginProduce("socket", new Message<Null, string>
                {
                    Value = JsonConvert.SerializeObject(new BinanceStreamClick(e) )
                }, Void);
                _producer.Flush(TimeSpan.FromSeconds(42));
            }
            catch (Exception ex)
            {
              //  Console.WriteLine(ex.ToString());
            }
        }







    }
}
