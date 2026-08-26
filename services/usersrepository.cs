#nullable enable
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

        /// <summary>
        /// Runs a read-modify-write cycle under a single lock, so concurrent callers
        /// (admin actions, self-service token regen, the hourly notification pass)
        /// can't interleave and clobber each other's writes.
        /// </summary>
        public void Mutate(Action<List<LampacUser>> action)
        {
            lock (_lock)
            {
                var users = ReadAll();
                action(users);
                WriteAll(users);
            }
        }

        public void AddOrReplace(LampacUser user)
        {
            Mutate(list =>
            {
                list.RemoveAll(u => u.TgId == user.TgId);
                list.Add(user);
            });
        }

        public void RemoveById(string lampacId)
        {
            Mutate(list => list.RemoveAll(u => u.Id == lampacId));
        }

        public LampacUser? SetExpiry(string lampacId, string expiresIso)
        {
            LampacUser? updated = null;
            Mutate(list =>
            {
                var u = list.FirstOrDefault(x => x.Id == lampacId);
                if (u == null) return;
                u.Expires = expiresIso;
                updated = u;
            });
            return updated;
        }

        public LampacUser? UpdateParams(string lampacId, Action<LampacUserParams> mutate)
        {
            LampacUser? updated = null;
            Mutate(list =>
            {
                var u = list.FirstOrDefault(x => x.Id == lampacId);
                if (u == null) return;
                mutate(u.Params);
                updated = u;
            });
            return updated;
        }

        /// <summary>
        /// Bulk-extends every user matching the predicate to the same expiry, returning how many were touched.
        /// </summary>
        public int ExtendWhere(Func<LampacUser, bool> predicate, string expiresIso)
        {
            int affected = 0;
            Mutate(list =>
            {
                foreach (var u in list)
                {
                    if (!predicate(u)) continue;
                    u.Expires = expiresIso;
                    affected++;
                }
            });
            return affected;
        }

        public LampacUser? Remove(string lampacId)
        {
            LampacUser? removed = null;
            Mutate(list =>
            {
                removed = list.FirstOrDefault(u => u.Id == lampacId);
                if (removed != null) list.Remove(removed);
            });
            return removed;
        }

        /// <summary>
        /// Atomically swaps a user's Lampac connection id for a fresh, collision-checked one.
        /// </summary>
        public LampacUser? RegenerateToken(long tgId, out string oldId)
        {
            string captured = "";
            LampacUser? updated = null;
            Mutate(list =>
            {
                var u = list.FirstOrDefault(x => x.TgId == tgId);
                if (u == null) return;
                captured = u.Id;
                string newTok;
                do { newTok = LampacUser.GenerateToken(); } while (list.Any(x => x.Id == newTok));
                u.Id = newTok;
                updated = u;
            });
            oldId = captured;
            return updated;
        }

        public string GenerateUniqueToken()
        {
            lock (_lock)
            {
                var existing = ReadAll();
                string tok;
                do { tok = LampacUser.GenerateToken(); } while (existing.Any(u => u.Id == tok));
                return tok;
            }
        }

        public LampacUser? Find(string idArg)
        {
            idArg = idArg.TrimStart('@');
            var users = ReadAll();
            if (long.TryParse(idArg, out long tgId))
            {
                var byTg = users.FirstOrDefault(x => x.TgId == tgId);
                if (byTg != null) return byTg;
            }
            var byLampacId = users.FirstOrDefault(x => x.Id == idArg);
            if (byLampacId != null) return byLampacId;
            return users.FirstOrDefault(x =>
                x.Comment.Contains(idArg, StringComparison.OrdinalIgnoreCase));
        }

        public bool ExistsByTgId(long tgId) =>
            ReadAll().Any(u => u.TgId == tgId);

        public bool Exists(string lampacId) =>
            ReadAll().Any(u => u.Id == lampacId);

        public LampacUser? GetByTgId(long tgId) =>
            ReadAll().FirstOrDefault(u => u.TgId == tgId);

        public LampacUser? GetById(string lampacId) =>
            ReadAll().FirstOrDefault(u => u.Id == lampacId);
    }
}
