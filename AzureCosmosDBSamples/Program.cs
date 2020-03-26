using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;

namespace AzureCosmosDBSamples
{
    class Program
    {
        static void Main(string[] args)
        {
            CosmosWorker.ViewDatabase().Wait();
            CosmosWorker.CreateDatabase("demodb").Wait();
        }

        
    }
}
