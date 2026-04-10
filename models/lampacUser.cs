using System;
using System.Text.Json.Serialization;

namespace TelegramBot.Models
{
    public class LampacUser
    {
        [JsonPropertyName("id")]
        public string Id { get; set; } = "";

        [JsonPropertyName("group")]
        public int Group { get; set; } = 1;

        [JsonPropertyName("expires")]
        public string Expires { get; set; } = "";

        [JsonPropertyName("comment")]
        public string Comment { get; set; } = "";

        [JsonPropertyName("params")]
        public LampacUserParams Params { get; set; } = new();
    }

    public class LampacUserParams
    {
        [JsonPropertyName("adult")]
        public bool Adult { get; set; } = false;

        [JsonPropertyName("admin")]
        public bool Admin { get; set; } = false;
    }

    public class PendingRequest
    {
        public long TelegramId { get; set; }
        public string Username { get; set; } = "";
        public string FirstName { get; set; } = "";
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }
}
