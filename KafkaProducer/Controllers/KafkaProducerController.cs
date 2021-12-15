using Confluent.Kafka;
using KafkaProducer.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaProducer.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class KafkaProducerController : ControllerBase
    {
        private readonly ProducerConfig config = new ProducerConfig { BootstrapServers = "localhost:9092" };
        private readonly string topic = "simpletalk_topic";
        
        [HttpPost]
        public IActionResult Post([FromBody] SendingData message)
        {
            int i = 0;
            while (i < 100000)
            {
                i++;
                message.Id++;
                string serializedData = JsonConvert.SerializeObject(message);
                Created(string.Empty, SendToKafka(topic, serializedData));
            }
            return Ok();
        }
        private Object SendToKafka(string topic, string message)
        {
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    return producer.ProduceAsync(topic, new Message<Null, string> { Value = message })
                        .GetAwaiter()
                        .GetResult();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Oops, something went wrong: {e}");
                }
            }
            return null;
        }
    }
}
