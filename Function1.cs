using System;
using Azure.Storage.Queues.Models;
using Microsoft.Azure.Functions.Worker;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;

namespace ContactFunction
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        public Function1(ILogger<Function1> logger)
        {
            _logger = logger;
        }

        [Function(nameof(Function1))]
        public async Task Run([QueueTrigger("contacts", Connection = "AzureWebJobsStorage")] QueueMessage message)
        {
            _logger.LogInformation($"C# Queue trigger function processed: {message.MessageText}");

            string messageJson = message.MessageText;

            // Deserialize the message
            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };
            Contact? contact = JsonSerializer.Deserialize<Contact>(messageJson, options);

            if (contact == null)
            {
                _logger.LogError("Failed to deserialize the message");
                return;
            }

            _logger.LogInformation($"contact: {contact.FirstName} {contact.LastName}"); // Fixed typo here too

            // get connection string from app settings
            string? connectionString = Environment.GetEnvironmentVariable("SqlConnectionString");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("SQL connection string is not set in the environment variables.");
            }

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                await conn.OpenAsync();

                var query = "INSERT INTO Contacts (FirstName, LastName, Email) VALUES (@FirstName, @LastName, @Email)";

                using (SqlCommand cmd = new SqlCommand(query, conn))
                {
                    cmd.Parameters.AddWithValue("@FirstName", contact.FirstName);
                    cmd.Parameters.AddWithValue("@LastName", contact.LastName); // Fixed the double @
                    cmd.Parameters.AddWithValue("@Email", contact.Email);

                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }
    }
}