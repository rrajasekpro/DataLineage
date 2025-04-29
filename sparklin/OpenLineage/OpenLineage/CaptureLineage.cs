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
using System.Collections.Generic;

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

            // Read and log request body
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

            // Extract fields with null checks
            eventType = eventType ?? data?.eventType;
            string runId = data?.run?.runId;
            string notebookName = data?.job?.name;
            string className = null;

            // Safe className extraction
            try
            {
                className = data?.run?.facets?["spark.logicalPlan"]?.plan?[0]?.@class;
                log.LogInformation($"Extracted class name: {className}");  // Logging class name for debugging
            }
            catch (Exception ex)
            {
                log.LogWarning($"Could not extract class name from plan: {ex.Message}");
            }

            // Check required fields
            if (string.IsNullOrEmpty(runId) || string.IsNullOrEmpty(notebookName))
            {
                log.LogError("Missing runId or notebookName.");
                return new BadRequestObjectResult("Missing required fields: runId or notebookName.");
            }

            // Trim notebook name if possible
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

            // Check event type and class
            string[] predefinedClassList = {
                "org.apache.spark.sql.execution.datasources.CreateTable",
                "org.apache.spark.sql.catalyst.plans.logical.CreateViewStatement",
                "org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelectStatement",
                "org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement",  // Added class here
                "org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand",
                "org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable"
            };

            if (eventType == "COMPLETE" && predefinedClassList.Contains(className))
            {
                // Prepare metadata
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

                    // Log or save event metadata (stubbed method)
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

                    log.LogInformation("File uploaded and metadata stored.");
                    return new OkObjectResult("File uploaded successfully.");
                }
                catch (Exception ex)
                {
                    log.LogError($"Storage error: {ex.Message}");
                    return new StatusCodeResult(500);
                }
            }
            else
            {
                log.LogInformation("Event type not COMPLETE or class name not in list.");
                return new OkObjectResult("Event Type is not COMPLETE or ClassName not matched.");
            }
        }
    }
}
