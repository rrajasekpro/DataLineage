using System;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System.Linq;

namespace OpenLineage
{
    public static class CaptureLineage
    {
        [FunctionName("CaptureLineage")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "1/lineage")] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function started processing a request.");

            string eventType = req.Query["eventType"];
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            if (string.IsNullOrEmpty(requestBody))
            {
                log.LogWarning("Empty request body.");
                return new BadRequestObjectResult("Request body is empty.");
            }

            dynamic data;
            try
            {
                data = JsonConvert.DeserializeObject(requestBody);
            }
            catch (Exception ex)
            {
                log.LogError($"Failed to parse JSON: {ex.Message}");
                return new BadRequestObjectResult("Invalid JSON format.");
            }

            eventType = eventType ?? data?.eventType;
            string runId = data?.run?.runId;
            string notebookName = data?.job?.name;
            string className = null;

            try
            {
                // Extract class name safely
                var planArray = data?.run?.facets?["spark.logicalPlan"]?.plan;
                if (planArray != null && planArray.HasValues && planArray[0]?["@class"] != null)
                {
                    className = planArray[0]["@class"].ToString();
                }
            }
            catch (Exception ex)
            {
                log.LogWarning($"Could not extract class name from logical plan: {ex.Message}");
            }

            if (string.IsNullOrEmpty(runId) || string.IsNullOrEmpty(notebookName))
            {
                log.LogError("Missing runId or notebookName.");
                return new BadRequestObjectResult("Missing required fields: runId or notebookName.");
            }

            try
            {
                if (notebookName.Contains("."))
                {
                    notebookName = notebookName.Substring(0, notebookName.IndexOf("."));
                }
            }
            catch (Exception ex)
            {
                log.LogWarning($"Failed to trim notebook name: {ex.Message}");
            }

            string[] predefinedClassList = {
                "org.apache.spark.sql.execution.datasources.CreateTable",
                "org.apache.spark.sql.catalyst.plans.logical.CreateViewStatement",
                "org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelectStatement",
                "org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement",
                "org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand",
                "org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable"
            };

            if (eventType == "COMPLETE" && predefinedClassList.Contains(className))
            {
                string currentTimestamp = DateTime.UtcNow.ToString("yyyyMMddHHmmss");
                string fileName = $"{runId}_{notebookName}_{currentTimestamp}.json";

                string connectionString = Environment.GetEnvironmentVariable("ConnectionString");
                string containerName = Environment.GetEnvironmentVariable("ContainerName");

                if (string.IsNullOrEmpty(connectionString) || string.IsNullOrEmpty(containerName))
                {
                    log.LogError("Missing storage configuration.");
                    return new StatusCodeResult(500);
                }

                try
                {
                    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
                    CloudBlobClient client = storageAccount.CreateCloudBlobClient();
                    CloudBlobContainer container = client.GetContainerReference(containerName);

                    CloudBlockBlob blob = container.GetBlockBlobReference(fileName);
                    blob.Properties.ContentType = "application/json";

                    using (Stream stream = new MemoryStream(Encoding.UTF8.GetBytes(requestBody)))
                    {
                        await blob.UploadFromStreamAsync(stream);
                    }

                    EventMetadata eventMetadata = new EventMetadata(
                        Utility.getQualifierName(notebookName),
                        $"{runId}_{notebookName}_{currentTimestamp}"
                    )
                    {
                        Status = Constant.UN_PROCESSED,
                        RetryCount = Constant.RETRY_COUNT,
                        isArchived = Constant.IS_ARCHIVE,
                        FilePath = $"{containerName}/{fileName}"
                    };

                    TableStorage tableStorage = new TableStorage();
                    tableStorage.insetEventMetadata(eventMetadata);

                    log.LogInformation("File uploaded and metadata saved.");
                    return new OkObjectResult("File uploaded successfully.");
                }
                catch (Exception ex)
                {
                    log.LogError($"Storage operation failed: {ex.Message}");
                    return new StatusCodeResult(500);
                }
            }
            else
            {
                log.LogInformation("Event type not COMPLETE or class name not recognized.");
                return new OkObjectResult("Event Type is not COMPLETE or ClassName not matched.");
            }
        }
    }
}
