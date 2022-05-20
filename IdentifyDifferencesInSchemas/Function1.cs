using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using Azure.Identity;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;
using System.Text;
using System.Net.Http;
using System.Text.Json;
using System.Configuration;
using System.Text.RegularExpressions;

namespace IdentifyDifferencesInSchemas
{

    public static class Function1
    {

        private static string SA1ConnectionString = Environment.GetEnvironmentVariable("SA1ConnectionString", EnvironmentVariableTarget.Process);

        private static string ConnectionStringToSendEmail = Environment.GetEnvironmentVariable("ConnectionStringToSendEmail", EnvironmentVariableTarget.Process);

        private static Dictionary<string, string> _previousSchema = new Dictionary<string, string>();

        private static Dictionary<string, string> _newSchema = new Dictionary<string, string>();

        private static string _containerName = "script";

        private static string _fileWithPreviousSchema = "d1OldSchema.txt";

        private static string _nameOfDifferenceFile = "diff.txt";

        //key = name of table/function
        //first value in tuple = previous schema version, second value in tuple = new schema version 
        private static Dictionary<string, (string, string)> _SchemaDifferences = new Dictionary<string, (string, string)>();



        //name - holds the name of the file found in the blob container.
        //myBlob - holds the content of the file.
        [FunctionName("Function1")]
        public static void Run([BlobTrigger("script/{name}", Connection = "SA1ConnectionString")]Stream myBlob, string name, ILogger log)
        {

            if (!name.Contains("d1OldSchema") && !name.Contains("diff"))//updating the old schema file will also trigger this function to run
            {

                LoadNewSchemaIntoDictionary(myBlob);

                LoadPreviousSchemaIntoDictionary();

                ReplaceOldSchemaBlob(name);

                PopulateDictionarytWithSchemaDiffs();

                CreateDiffFileAndSendToSA();

                DeleteNewSchemaFile(name);

                _previousSchema.Clear();

                _newSchema.Clear();

                _SchemaDifferences.Clear();
            }
        }

        private static void LoadPreviousSchemaIntoDictionary()
        {

            BlobServiceClient blobServiceClient = new BlobServiceClient(SA1ConnectionString);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(_containerName);
            BlobClient blobClient = containerClient.GetBlobClient(_fileWithPreviousSchema);

            if (blobClient.Exists())
            {
                var response = blobClient.Download();

                bool isFirstLine = true;

                string name = "";

                StringBuilder currentElement = new StringBuilder();

                using (var streamReader = new StreamReader(response.Value.Content))
                {
                    while (!streamReader.EndOfStream)
                    {

                       var line = streamReader.ReadLine();

                       if (line.StartsWith(".create") && !isFirstLine)
                       {
                            try
                            {

                                name = GetObjectNameFromKustoCommand(currentElement.ToString());

                                _previousSchema[name] = currentElement.ToString();

                                currentElement.Clear();
                            }
                            catch (Exception e)
                            { }
                       }

                       isFirstLine = false;

                       currentElement.Append(line + "\n");

                    }

                    if (currentElement.Length > 0)
                    {
                        try
                        {

                            name = GetObjectNameFromKustoCommand(currentElement.ToString());

                            _previousSchema[name] = currentElement.ToString();
                        }
                        catch (Exception e)
                        { }
                    }
                }
  
            }

        }

        private static string GetObjectNameFromKustoCommand(string command)
        {
            string name = "";

            if (command.Contains("function"))
            {
                string functionRegex = @".create-or-alter function with \((?<storedProcedureConfig>[^{}]+)\)\s+(?<storedProcedureName>[a-zA-Z0-9\.-_]+)\s*\((?<storedProcedureParameters>[^{}]*)\)\s*{(?<storedProcedureBody>[\s\S]*)}";
                Regex createOrAlterFunctionRegex = new Regex(functionRegex);
                Match createOrAlterFunctionMatch = createOrAlterFunctionRegex.Match(command);

                if (createOrAlterFunctionMatch.Success)
                {
                    name = "Function: " + createOrAlterFunctionMatch.Groups["storedProcedureName"].Value;
                }
                else
                {
                    string functionRegexCreate = @".create function with \((?<storedProcedureConfig>[^{}]+)\)\s+(?<storedProcedureName>[a-zA-Z0-9\.-_]+)\s*\((?<storedProcedureParameters>[^{}]*)\)\s*{(?<storedProcedureBody>[\s\S]*)}";
                    Regex createFunctionRegex = new Regex(functionRegexCreate);
                    Match createFunctionMatch = createOrAlterFunctionRegex.Match(command);

                    if (createFunctionMatch.Success)
                    {
                        name = "Function: " + createFunctionMatch.Groups["storedProcedureName"].Value;
                    }
                }

            }
            else
            {
                string tableRegex = @".create-merge table (?<tableName>\[?'?[\w\.]+'?\]?) \((?<tableParameters>[^\\)]+)\)( with \(?(?<tableConfig>[^)]+)\))?";
                Regex createMergeTableRegex = new Regex(tableRegex);
                Match createMergeTableMatch = createMergeTableRegex.Match(command);

                if (createMergeTableMatch.Success)
                {
                        name = "Table: " + createMergeTableMatch.Groups["tableName"].Value;
                }
                else
                {
                    string tableRegexCreate = @".create table (?<tableName>\[?'?[\w\.]+'?\]?) \((?<tableParameters>[^\\)]+)\)( with \(?(?<tableConfig>[^)]+)\))?";
                    Regex createTableRegex = new Regex(tableRegexCreate);
                    Match createTableMatch = createTableRegex.Match(command);

                    if (createTableMatch.Success)
                    {
                        name = "Table: " + createTableMatch.Groups["tableName"].Value;
                    }
                }

            }

            return name;

        }

        private static void LoadNewSchemaIntoDictionary(Stream stream)
        {
            using (var streamReader = new StreamReader(stream))
            {
                bool isFirstLine = true;

                string name = "";

                StringBuilder currentElement = new StringBuilder();

                while (!streamReader.EndOfStream)
                {

                    var line = streamReader.ReadLine();

                    if (line.StartsWith(".create") && !isFirstLine)
                    {

                        try
                        {
                            name = GetObjectNameFromKustoCommand(currentElement.ToString());

                            _newSchema[name] = currentElement.ToString();

                            currentElement.Clear();
                        }
                        catch (Exception e)
                        { }

                        
                    }

                    isFirstLine = false;

                    currentElement.Append(line + "\n");

                }

                if (currentElement.Length > 0)
                {

                    try
                    {
                        name = GetObjectNameFromKustoCommand(currentElement.ToString());

                        _newSchema[name] = currentElement.ToString();

                    }
                    catch (Exception e)
                    { }
                }
            }
          
        }

        private static void ReplaceOldSchemaBlob(string name)
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(SA1ConnectionString);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(_containerName);
            BlobClient blobClient = containerClient.GetBlobClient(name);
            BlobClient blobClientNewName = containerClient.GetBlobClient(_fileWithPreviousSchema);

            blobClientNewName.StartCopyFromUri(blobClient.Uri);
        }
        
        private static void PopulateDictionarytWithSchemaDiffs()
        {

            foreach (KeyValuePair<string,string> kvp in _newSchema)
            {
                if (!_previousSchema.ContainsKey(kvp.Key))
                {
                    _SchemaDifferences[kvp.Key] = ("New Object", kvp.Value);
                }
                else
                {
                    if (!_previousSchema[kvp.Key].Equals(_newSchema[kvp.Key]))
                    {
                        _SchemaDifferences[kvp.Key] = (_previousSchema[kvp.Key], _newSchema[kvp.Key]);
                    }
                }
            }
  
        }

        private static void CreateDiffFileAndSendToSA()
        {

            //Connects to the SA configured with teleport for replication 
            BlobServiceClient blobServiceClient = new BlobServiceClient(SA1ConnectionString);//will eventually be changed to the connection string for the SA configured with teleport 
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(_containerName);
            BlobClient diffBlob = containerClient.GetBlobClient(_nameOfDifferenceFile);

            StringBuilder diffString = new StringBuilder();

            foreach (KeyValuePair<string,(string,string)> kvp in _SchemaDifferences)
            {
                diffString.Append($"\n------ {kvp.Key} ------\n");

                diffString.Append("Previous Schema: \n" + kvp.Value.Item1 + "\n");

                diffString.Append("New Schema: \n" + kvp.Value.Item2 + "\n\n\n");

            }

            if (diffString.Length > 0)
            {
                diffBlob.DeleteIfExists();

                diffBlob.Upload(GenerateStreamFromString(diffString.ToString())); //creates new blob in SA and writes the string to it

                //send email to notify DRI of an update 
                SendPostToTriggerEmail();
            }
        }

        private static void DeleteNewSchemaFile(string name)
        {
            BlobServiceClient blobServiceClient = new BlobServiceClient(SA1ConnectionString);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(_containerName);
            BlobClient blobClient = containerClient.GetBlobClient(name);


            blobClient.Delete();
        }

        private static Stream GenerateStreamFromString(string s)
        {
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.Write(s);
            writer.Flush();
            stream.Position = 0;
            return stream;
        }


        private async static void SendPostToTriggerEmail()
        {
            var client = new HttpClient();
            var jsonData = JsonSerializer.Serialize(new
            {
                email = "sheacooke@microsoft.com",
                task = "Automated Notification: Kusto Updates"
            });

             await client.PostAsync(ConnectionStringToSendEmail, new StringContent(jsonData, Encoding.UTF8, "application/json"));

        }
    }
}
