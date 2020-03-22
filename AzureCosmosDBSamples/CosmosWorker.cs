using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AzureCosmosDBSamples
{
    public static class CosmosWorker
	{
		public static CosmosClient Client { get; private set; }

		static CosmosWorker()
		{
			var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
            var endpoint = config["CosmosEndpoint"];
            var masterKey = config["CosmosMasterKey"];

			Client = new CosmosClient(endpoint, masterKey);
		}

		public async static Task ViewDatabase()
		{
			Console.WriteLine();
			Console.WriteLine(">>> View Databases <<<");
			var iterator = CosmosWorker.Client.GetDatabaseQueryIterator<DatabaseProperties>();
			var databases = await iterator.ReadNextAsync();

			var count = 0;
			foreach (var database in databases)
			{
				count++;
				Console.WriteLine($" Database Id: {database.Id}; Modified: {database.LastModified}");
			}

			Console.WriteLine();
			Console.WriteLine($"Total databases: {count}");

		}

		public async static Task CreateDatabase()
		{
			Console.WriteLine();
			Console.WriteLine(">>> Create Database <<<");

			var result = await CosmosWorker.Client.CreateDatabaseAsync("MyNewDatabase");
			var database = result.Resource;

			Console.WriteLine($" Database Id: {database.Id}; Modified: {database.LastModified}");
		}

		public async static Task DeleteDatabase()
		{
			Console.WriteLine();
			Console.WriteLine(">>> Delete Database <<<");

			await CosmosWorker.Client.GetDatabase("MyNewDatabase").DeleteAsync();
		}

		public static async Task ViewContainers(string dbname)
		{
			Console.WriteLine();
			Console.WriteLine(">>> View Containers in mydb <<<");

			var database = CosmosWorker.Client.GetDatabase(dbname);
			var iterator = database.GetContainerQueryIterator<ContainerProperties>();
			var containers = await iterator.ReadNextAsync();

			var count = 0;
			foreach (var container in containers)
			{
				count++;
				Console.WriteLine();
				Console.WriteLine($" Container #{count}");
				await ViewContainer(container,dbname);
			}

			Console.WriteLine();
			Console.WriteLine($"Total containers in {dbname} database: {count}");
		}

		public async static Task ViewContainer(ContainerProperties containerProperties, string dbname)
		{
			Console.WriteLine($"     Container ID: {containerProperties.Id}");
			Console.WriteLine($"    Last Modified: {containerProperties.LastModified}");
			Console.WriteLine($"    Partition Key: {containerProperties.PartitionKeyPath}");

			var container = CosmosWorker.Client.GetContainer(dbname, containerProperties.Id);
			var throughput = await container.ReadThroughputAsync();

			Console.WriteLine($"       Throughput: {throughput}");
		}

		public async static Task CreateContainer(
			string containerId,
			string dbname,
			int throughput = 400,
			string partitionKey = "/partitionKey")
		{
			Console.WriteLine();
			Console.WriteLine($">>> Create Container {containerId} in mydb <<<");
			Console.WriteLine();
			Console.WriteLine($" Throughput: {throughput} RU/sec");
			Console.WriteLine($" Partition key: {partitionKey}");
			Console.WriteLine();

			var containerDef = new ContainerProperties
			{
				Id = containerId,
				PartitionKeyPath = partitionKey,
			};

			var database = CosmosWorker.Client.GetDatabase("mydb");
			var result = await database.CreateContainerAsync(containerDef, throughput);
			var container = result.Resource;

			Console.WriteLine("Created new container");
			await ViewContainer(container,dbname); // Intermittent failures!
		}

		public async static Task DeleteContainer(string containerId)
		{
			Console.WriteLine();
			Console.WriteLine($">>> Delete Container {containerId} in mydb <<<");

			var container = CosmosWorker.Client.GetContainer("mydb", containerId);
			await container.DeleteContainerAsync();

			Console.WriteLine($"Deleted container {containerId} from database mydb");
		}

	}
}
