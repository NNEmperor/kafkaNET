﻿using Confluent.Kafka;
using Confluent.Kafka.Admin;
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
        private readonly ProducerConfig config = new ProducerConfig { BootstrapServers = "localhost:9092,localhost:9093" };
        private readonly string topic = "simpletalk_topic_multiple";
        private static bool isFirst = true;

        [HttpPost]
        public IActionResult Post([FromBody] SendingData message)
        {
            int i = 0;
            //while (i < 100000)
            {
                i++;
                message.Id++;
                string serializedData = JsonConvert.SerializeObject(message);
                Created(string.Empty, SendToKafkaAsync(topic, serializedData));
            }
            return Ok();
        }
        private async Task<object> SendToKafkaAsync(string topic, string message)
        {
            if (isFirst)
            {
                using (var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = "localhost:9092,localhost:9093" }).Build())
                {
                    try
                    {
                        await adminClient.CreateTopicsAsync(new TopicSpecification[] {
                    new TopicSpecification { Name = "simpletalk_topic_multiple", ReplicationFactor = 2, NumPartitions = 1 } });
                        isFirst = false;
                    }
                    catch (CreateTopicsException e)
                    {
                        Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
                    }
                }
            }


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
