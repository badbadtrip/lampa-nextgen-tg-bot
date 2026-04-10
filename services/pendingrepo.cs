using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using TelegramBot.Models;

namespace TelegramBot.Services
{
    /// <summary>
    /// Thread-safe, disk-backed store for pending access requests.
    /// Survives bot restarts — requests are reloaded from pending.json on startup.
    /// </summary>
    public class PendingRepository
    {
        readonly string _path;
        readonly object _lock = new();
        Dictionary<int, PendingRequest> _cache;

        static readonly JsonSerializerOptions _writeOpts = new() { WriteIndented = true };

        public PendingRepository(string path)
        {
            _path  = path;
            _cache = Load();
        }

        Dictionary<int, PendingRequest> Load()
        {
            try
            {
                if (!File.Exists(_path)) return new();
                var list = JsonSerializer.Deserialize<List<PendingEntry>>(File.ReadAllText(_path)) ?? new();
                return list.ToDictionary(
                    e => e.AdminMessageId,
                    e => new PendingRequest
                    {
                        TelegramId = e.TelegramId,
                        Username   = e.Username,
                        FirstName  = e.FirstName,
                        CreatedAt  = e.CreatedAt
                    });
            }
            catch { return new(); }
        }

        void Save()
        {
            try
            {
                var list = _cache.Select(kv => new PendingEntry
                {
                    AdminMessageId = kv.Key,
                    TelegramId     = kv.Value.TelegramId,
                    Username       = kv.Value.Username,
                    FirstName      = kv.Value.FirstName,
                    CreatedAt      = kv.Value.CreatedAt
                }).ToList();
                File.WriteAllText(_path, JsonSerializer.Serialize(list, _writeOpts));
            }
            catch { }
        }

        public void Add(int adminMsgId, PendingRequest req)
        {
            lock (_lock) { _cache[adminMsgId] = req; Save(); }
        }

        public bool HasPendingFor(long userId)
        {
            lock (_lock) return _cache.Values.Any(p => p.TelegramId == userId);
        }

        /// <summary>
        /// Finds and removes a pending entry: first by the admin message ID (normal flow),
        /// then falls back to searching by user ID (pending-list approve/deny flow).
        /// </summary>
        public bool TryGetAndRemove(int msgId, long userId, out PendingRequest? req)
        {
            lock (_lock)
            {
                if (_cache.TryGetValue(msgId, out req))
                {
                    _cache.Remove(msgId);
                    Save();
                    return true;
                }
                var kv = _cache.FirstOrDefault(p => p.Value.TelegramId == userId);
                if (kv.Value != null)
                {
                    req = kv.Value;
                    _cache.Remove(kv.Key);
                    Save();
                    return true;
                }
                return false;
            }
        }

        public IReadOnlyList<(int AdminMsgId, PendingRequest Req)> GetAll()
        {
            lock (_lock)
                return _cache.Select(kv => (kv.Key, kv.Value)).ToList();
        }

        public int Count { get { lock (_lock) return _cache.Count; } }
    }

    public class PendingEntry
    {
        [JsonPropertyName("adminMsgId")] public int AdminMessageId { get; set; }
        [JsonPropertyName("telegramId")] public long TelegramId { get; set; }
        [JsonPropertyName("username")]   public string Username  { get; set; } = "";
        [JsonPropertyName("firstName")]  public string FirstName { get; set; } = "";
        [JsonPropertyName("createdAt")]  public DateTime CreatedAt { get; set; }
    }
}
