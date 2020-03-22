using System;
using System.Collections.Generic;
using System.Text;

namespace AzureCosmosDBSamples
{
    //Incomplete !!!!!!!!
    class ListDocuments
    {
        private async static Task QueryForDocuments()
        {
            var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
            var endpoint = config["CosmosEndpoint"];
            var masterkey = config["CosmosMasterKey"];
            var client = new CDBClient.Client(endpoint, masterkey);


            var container = client.GetContainer("Families", "Families");
            var sql = "SELECT * FROM c WHERE ARRAY_LENGHT(c.kids) > 1";
            var iterator = container.GetItemQueryIterator<dynamic>(sql);
            var page = await iterator.ReadNextAsync();

            foreach (var doc in page)
            {
                Console.WriteLine($"Family {doc.id} has {doc.kids.Count} children.");
            }
            Console.ReadLine();


        }
    }

}