using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;
using Microsoft.Extensions.Logging;
using TelegramBot.Models;

namespace TelegramBot.Services
{
    public class UsersRepository
    {
        readonly string _path;
        readonly object _lock = new();
        readonly ILogger<UsersRepository> _logger;

        static readonly JsonSerializerOptions _writeOpts = new()
        {
            WriteIndented = true,
            Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
        };

        public UsersRepository(string path, ILogger<UsersRepository> logger)
        {
            _path   = path;
            _logger = logger;
        }

        public List<LampacUser> ReadAll()
        {
            lock (_lock)
            {
                try
                {
                    if (!File.Exists(_path)) return new();
                    var json = File.ReadAllText(_path);
                    return JsonSerializer.Deserialize<List<LampacUser>>(json) ?? new();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[TelegramBot] ReadUsers error");
                    return new();
                }
            }
        }

        public void WriteAll(List<LampacUser> users)
        {
            lock (_lock)
            {
                try
                {
                    var json = JsonSerializer.Serialize(users, _writeOpts);
                    File.WriteAllText(_path, json);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[TelegramBot] WriteUsers error");
                }
            }
        }

        public void AddOrReplace(LampacUser user)
        {
            var users = ReadAll();
            users.RemoveAll(u => u.Id == user.Id);
            users.Add(user);
            WriteAll(users);
        }

        public LampacUser? Find(string idArg)
        {
            idArg = idArg.TrimStart('@');
            var users = ReadAll();
            var u = users.FirstOrDefault(x => x.Id == idArg);
            if (u != null) return u;
            return users.FirstOrDefault(x =>
                x.Comment.Contains(idArg, StringComparison.OrdinalIgnoreCase));
        }

        public bool Exists(string telegramId) =>
            ReadAll().Any(u => u.Id == telegramId);

        public LampacUser? GetById(string telegramId) =>
            ReadAll().FirstOrDefault(u => u.Id == telegramId);
    }
}
