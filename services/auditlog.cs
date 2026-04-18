using System;
using System.IO;
using Microsoft.Extensions.Logging;

namespace TelegramBot.Services
{
    public class AuditLog
    {
        readonly string _path;
        readonly object _lock = new();
        readonly ILogger<AuditLog> _logger;

        public AuditLog(string path, ILogger<AuditLog> logger)
        {
            _path   = path;
            _logger = logger;
        }

        public void Write(string action, long tgId, string comment, string? details = null)
        {
            var line = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss") + "  " +
                       action.PadRight(16) + "  tg=" + tgId + "  \"" + comment + "\"" +
                       (details != null ? "  " + details : "");
            lock (_lock)
            {
                try { File.AppendAllText(_path, line + "\n"); }
                catch (Exception ex) { _logger.LogWarning("[TelegramBot] AuditLog write failed: {Msg}", ex.Message); }
            }
        }
    }
}
