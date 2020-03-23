using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
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

		public async static Task CreateDatabase(string dbname)
		{
			Console.WriteLine();
			Console.WriteLine(">>> Create Database <<<");

			var result = await CosmosWorker.Client.CreateDatabaseAsync(dbname);
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

			var database = CosmosWorker.Client.GetDatabase(dbname);
			var result = await database.CreateContainerAsync(containerDef, throughput);
			var container = result.Resource;

			Console.WriteLine("Created new container");
			await ViewContainer(container,dbname); // Intermittent failures!
		}

		public async static Task DeleteContainer(string containerId, string dbname)
		{
			Console.WriteLine();
			Console.WriteLine($">>> Delete Container {containerId} in {dbname} <<<");

			var container = CosmosWorker.Client.GetContainer(dbname, containerId);
			await container.DeleteContainerAsync();

			Console.WriteLine($"Deleted container {containerId} from database {dbname}");
		}

		public async static Task CreateDocuments()
		{
			Console.Clear();
			Console.WriteLine(">>> Create Documents <<<");
			Console.WriteLine();

			var container = CosmosWorker.Client.GetContainer("mydb", "mystore");

			dynamic document1Dynamic = new
			{
				id = Guid.NewGuid(),
				name = "New Customer 1",
				address = new
				{
					addressType = "Microsoft HQ",
					addressLine1 = "One Microsoft Way",
					location = new
					{
						city = "Redmond",
						stateProvinceName = "Washington"
					},
					postalCode = "11229",
					countryRegionName = "United States"
				},
			};

			await container.CreateItemAsync(document1Dynamic, new PartitionKey("11229"));
			Console.WriteLine($"Created new document {document1Dynamic.id} from dynamic object");

			var document2Json = $@"
				{{
					""id"": ""{Guid.NewGuid()}"",
					""name"": ""New Customer 2"",
					""address"": {{
						""addressType"": ""Main Office"",
						""addressLine1"": ""123 Main Street"",
						""location"": {{
							""city"": ""Beverly Hills""
							""stateProvinceName"": ""Los Angeles""
						}},
						""postalCode"": ""11229"",
						""countryRegionName"": ""United States""
					}}
				}}";

			var document2Object = JsonConvert.DeserializeObject<JObject>(document2Json);
			await container.CreateItemAsync(document2Object, new PartitionKey("11229"));
			Console.WriteLine($"Created new document {document2Object["id"].Value<string>()} from JSON string");

			var document3Poco = new Customer
			{
				Id = Guid.NewGuid().ToString(),
				Name = "New Customer 3",
				Address = new Address
				{
					AddressType = "Main Office",
					AddressLine1 = "123 Main Street",
					Location = new Location
					{
						City = "Brooklyn",
						StateProvinceName = "New York"
					},
					PostalCode = "11229",
					CountryRegionName = "United States"
				},
			};

			await container.CreateItemAsync(document3Poco, new PartitionKey("11229"));
			Console.WriteLine($"Created new document {document3Poco.Id} from typed object");
		}

		public static async Task QueryDocuments()
		{
			Console.Clear();
			Console.WriteLine(">>> Query Documents (SQL) <<<");
			Console.WriteLine();

			var container = CosmosWorker.Client.GetContainer("mydb", "mystore");

			Console.WriteLine("Querying for new customer documents (SQL)");
			var sql = "SELECT * FROM c WHERE STARTSWITH(c.name, 'New Customer') = true";

			// Query for dynamic objects
			var iterator1 = container.GetItemQueryIterator<dynamic>(sql);
			var documents1 = await iterator1.ReadNextAsync();
			var count = 0;
			foreach (var document in documents1)
			{
				Console.WriteLine($" ({++count}) Id: {document.id}; Name: {document.name};");

				// Dynamic object can be converted into a defined type...
				var customer = JsonConvert.DeserializeObject<Customer>(document.ToString());
				Console.WriteLine($"     City: {customer.Address.Location.City}");
			}
			Console.WriteLine($"Retrieved {count} new documents as dynamic");
			Console.WriteLine();

			// Or query for defined types; e.g., Customer
			var iterator2 = container.GetItemQueryIterator<Customer>(sql);
			var documents2 = await iterator2.ReadNextAsync();
			count = 0;
			foreach (var customer in documents2)
			{
				Console.WriteLine($" ({++count}) Id: {customer.Id}; Name: {customer.Name};");
				Console.WriteLine($"     City: {customer.Address.Location.City}");
			}
			Console.WriteLine($"Retrieved {count} new documents as Customer");
			Console.WriteLine();

			// You only get back the first "page" (up to MaxItemCount)
		}

		public async static Task DeleteDocuments()
		{
			Console.Clear();
			Console.WriteLine(">>> Delete Documents <<<");
			Console.WriteLine();

			var container = CosmosWorker.Client.GetContainer("mydb", "mystore");

			Console.WriteLine("Querying for documents to be deleted");
			var sql = "SELECT c.id, c.address.postalCode FROM c WHERE STARTSWITH(c.name, 'New Customer') = true";
			var iterator = container.GetItemQueryIterator<dynamic>(sql);
			var documents = (await iterator.ReadNextAsync()).ToList();
			Console.WriteLine($"Found {documents.Count} documents to be deleted");
			foreach (var document in documents)
			{
				string id = document.id;
				string pk = document.postalCode;
				await container.DeleteItemAsync<dynamic>(id, new PartitionKey(pk));
			}
			Console.WriteLine($"Deleted {documents.Count} new customer documents");
			Console.WriteLine();
		}

		public async static Task ReplaceDocuments()
		{
			Console.Clear();
			Console.WriteLine(">>> Replace Documents <<<");
			Console.WriteLine();

			var container = CosmosWorker.Client.GetContainer("mydb", "mystore");

			Console.WriteLine("Querying for documents with 'isNew' flag");
			var sql = "SELECT VALUE COUNT(c) FROM c WHERE c.isNew = true";
			var count = (await (container.GetItemQueryIterator<int>(sql)).ReadNextAsync()).First();
			Console.WriteLine($"Documents with 'isNew' flag: {count}");
			Console.WriteLine();

			Console.WriteLine("Querying for documents to be updated");
			sql = "SELECT * FROM c WHERE STARTSWITH(c.name, 'New Customer') = true";
			var documents = (await (container.GetItemQueryIterator<dynamic>(sql)).ReadNextAsync()).ToList();
			Console.WriteLine($"Found {documents.Count} documents to be updated");
			foreach (var document in documents)
			{
				document.isNew = true;
				var result = await container.ReplaceItemAsync<dynamic>(document, (string)document.id);
				var updatedDocument = result.Resource;
				Console.WriteLine($"Updated document 'isNew' flag: {updatedDocument.isNew}");
			}
			Console.WriteLine();

			Console.WriteLine("Querying for documents with 'isNew' flag");
			sql = "SELECT VALUE COUNT(c) FROM c WHERE c.isNew = true";
			count = (await (container.GetItemQueryIterator<int>(sql)).ReadNextAsync()).First();
			Console.WriteLine($"Documents with 'isNew' flag: {count}");
			Console.WriteLine();
		}

	}
}
