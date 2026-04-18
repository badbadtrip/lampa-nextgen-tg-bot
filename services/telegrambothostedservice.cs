using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Telegram.Bot;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;
using TelegramBot.Services;

namespace TelegramBot.Services
{
    public sealed class TelegramBotHostedService : BackgroundService
    {
        const int GetUpdatesLimit          = 100;
        const int GetUpdatesTimeoutSeconds = 50;
        static readonly TimeSpan ErrorDelay = TimeSpan.FromSeconds(5);

        readonly ILoggerFactory _loggerFactory;
        readonly ILogger<TelegramBotHostedService> _logger;

        public TelegramBotHostedService(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
            _logger        = loggerFactory.CreateLogger<TelegramBotHostedService>();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                await RunBotAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "[TelegramBot] Fatal error");
                throw;
            }
        }

        async Task RunBotAsync(CancellationToken ct)
        {
            var conf = ModInit.conf;

            if (!conf.enable || string.IsNullOrWhiteSpace(conf.bot_token))
            {
                _logger.LogInformation("[TelegramBot] Отключён (enable=false или пустой bot_token).");
                return;
            }

            if (conf.admin_id == 0)
            {
                _logger.LogWarning("[TelegramBot] admin_id не задан в init.conf — бот не запущен.");
                return;
            }

            var dir         = Path.GetDirectoryName(Path.GetFullPath(conf.users_file_path)) ?? ".";
            var pendingPath = Path.Combine(dir, "pending.json");
            var notifPath   = Path.Combine(dir, "notifications_date.txt");
            var auditPath   = Path.Combine(dir, "audit.log");

            var repo        = new UsersRepository(conf.users_file_path, _loggerFactory.CreateLogger<UsersRepository>());
            var pendingRepo = new PendingRepository(pendingPath);
            var auditLog    = new AuditLog(auditPath, _loggerFactory.CreateLogger<AuditLog>());
            var session     = new BotSession(conf, repo, pendingRepo, auditLog, _loggerFactory.CreateLogger<BotSession>(), notifPath);
            var bot         = new TelegramBotClient(conf.bot_token.Trim());

            try
            {
                var me = await bot.GetMe(ct).ConfigureAwait(false);
                _logger.LogInformation("[TelegramBot] @{Username} запущен. AdminId={AdminId}", me.Username, conf.admin_id);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[TelegramBot] GetMe не удался");
                return;
            }

            try
            {
                await bot.DeleteWebhook(dropPendingUpdates: false, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[TelegramBot] DeleteWebhook warning");
            }

            try
            {
                await bot.SetMyCommands(new[]
                {
                    new Telegram.Bot.Types.BotCommand { Command = "start", Description = "Главное меню" },
                    new Telegram.Bot.Types.BotCommand { Command = "help",  Description = "Помощь" }
                }, cancellationToken: ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "[TelegramBot] SetMyCommands warning");
            }

            _logger.LogInformation("[TelegramBot] Long polling запущен (limit={Limit}, timeout={Timeout}s).",
                GetUpdatesLimit, GetUpdatesTimeoutSeconds);

            var pollTask  = PollUpdatesAsync(bot, session, ct);
            var notifTask = RunNotificationTimerAsync(bot, session, ct);
            await Task.WhenAll(pollTask, notifTask).ConfigureAwait(false);
        }

        async Task PollUpdatesAsync(ITelegramBotClient bot, BotSession session, CancellationToken ct)
        {
            int? offset = null;

            while (!ct.IsCancellationRequested)
            {
                Update[] updates;
                try
                {
                    updates = await bot.GetUpdates(
                        offset,
                        limit: GetUpdatesLimit,
                        timeout: GetUpdatesTimeoutSeconds,
                        allowedUpdates: null,
                        cancellationToken: ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[TelegramBot] GetUpdates error");
                    try { await Task.Delay(ErrorDelay, ct).ConfigureAwait(false); }
                    catch (OperationCanceledException) { break; }
                    continue;
                }

                foreach (var update in updates)
                {
                    offset = update.Id + 1;
                    try
                    {
                        await session.HandleUpdateAsync(bot, update, ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "[TelegramBot] HandleUpdate error (UpdateId={UpdateId})", update.Id);
                    }
                }
            }
        }

        async Task RunNotificationTimerAsync(ITelegramBotClient bot, BotSession session, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await session.RunDailyNotificationsAsync(bot, ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[TelegramBot] Notification timer error");
                }

                try { await Task.Delay(TimeSpan.FromHours(1), ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { break; }
            }
        }
    }
}
