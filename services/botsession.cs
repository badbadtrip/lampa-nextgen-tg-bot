using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Telegram.Bot;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;
using Telegram.Bot.Types.ReplyMarkups;
using TelegramBot.Models;

namespace TelegramBot.Services
{
    public class BotSession
    {
        readonly TelegramBotConf _conf;
        readonly UsersRepository _repo;
        readonly PendingRepository _pendingRepo;
        readonly ILogger<BotSession> _logger;
        readonly string _notifPath;

        readonly Dictionary<long, string> _adminState = new();
        readonly object _stateLock = new();

        static readonly ReplyKeyboardMarkup AdminMenu = new(new[]
        {
            new KeyboardButton[] { "👥  Пользователи", "➕  Добавить",  "📊  Статистика" },
            new KeyboardButton[] { "🔍  Найти",         "📢  Рассылка",  "📋  Заявки"     }
        })
        { ResizeKeyboard = true };

        static readonly ReplyKeyboardMarkup UserMenu = new(new[]
        {
            new KeyboardButton[] { "📋  Мой профиль", "🆘  Поддержка" }
        })
        { ResizeKeyboard = true };

        const string BTN_USERS     = "👥  Пользователи";
        const string BTN_ADD       = "➕  Добавить";
        const string BTN_STATS     = "📊  Статистика";
        const string BTN_FIND      = "🔍  Найти";
        const string BTN_BROADCAST = "📢  Рассылка";
        const string BTN_PENDING   = "📋  Заявки";
        const string BTN_PROFILE   = "📋  Мой профиль";
        const string BTN_SUPPORT   = "🆘  Поддержка";

        public BotSession(TelegramBotConf conf, UsersRepository repo, PendingRepository pendingRepo,
                          ILogger<BotSession> logger, string notifPath)
        {
            _conf        = conf;
            _repo        = repo;
            _pendingRepo = pendingRepo;
            _logger      = logger;
            _notifPath   = notifPath;
        }

        // Correctly handles timezone offset stored in the date string
        static DateTime ParseExpiry(string expires) =>
            DateTimeOffset.TryParse(expires, CultureInfo.InvariantCulture,
                DateTimeStyles.RoundtripKind, out var dto)
                ? dto.UtcDateTime
                : DateTime.MinValue;

        static string StoreExpiry(DateTime dt) =>
            dt.ToString("yyyy-MM-ddT00:00:00+03:00");

        public async Task HandleUpdateAsync(ITelegramBotClient bot, Update update, CancellationToken ct)
        {
            if (update.CallbackQuery != null) { await HandleCallbackAsync(bot, update.CallbackQuery, ct); return; }
            if (update.Message is { } msg) await HandleMessageAsync(bot, msg, ct);
        }

        #region State helpers
        string? GetState(long uid) { lock (_stateLock) { _adminState.TryGetValue(uid, out var s); return s; } }
        void SetState(long uid, string s) { lock (_stateLock) { _adminState[uid] = s; } }
        void ClearState(long uid) { lock (_stateLock) { _adminState.Remove(uid); } }
        #endregion

        #region Message handling

        async Task HandleMessageAsync(ITelegramBotClient bot, Message msg, CancellationToken ct)
        {
            long userId  = msg.From?.Id ?? 0;
            bool isAdmin = userId == _conf.admin_id;
            var  text    = msg.Text?.Trim() ?? "";

            if (text.StartsWith("/start")) { await HandleStartAsync(bot, msg, isAdmin, ct); return; }

            if (!isAdmin)
            {
                if (text == BTN_PROFILE) { await ShowUserProfileAsync(bot, msg, ct); return; }
                if (text == BTN_SUPPORT) { await HandleSupportAsync(bot, msg, ct); return; }

                var existing = _repo.GetById(userId.ToString());
                if (existing == null)
                {
                    var kb = new InlineKeyboardMarkup(new[]
                    {
                        new[] { InlineKeyboardButton.WithCallbackData("🚀  Запросить доступ", "req:" + userId) }
                    });
                    await bot.SendMessage(msg.Chat.Id, "👋  Для получения доступа нажмите кнопку:",
                        replyMarkup: kb, cancellationToken: ct);
                }
                else
                {
                    await bot.SendMessage(msg.Chat.Id, "Выберите действие:", replyMarkup: UserMenu, cancellationToken: ct);
                }
                return;
            }

            var state = GetState(userId);
            if (state != null) { await HandleAdminInputAsync(bot, msg.Chat.Id, userId, state, text, ct); return; }

            if (text == BTN_USERS)     { await ShowUserListAsync(bot, msg.Chat.Id, 0, ct); return; }
            if (text == BTN_STATS)     { await ShowStatsAsync(bot, msg.Chat.Id, ct); return; }
            if (text == BTN_ADD)       { await StartAddUserAsync(bot, msg.Chat.Id, userId, ct); return; }
            if (text == BTN_FIND)      { await StartFindAsync(bot, msg.Chat.Id, userId, ct); return; }
            if (text == BTN_BROADCAST) { await StartBroadcastAsync(bot, msg.Chat.Id, userId, ct); return; }
            if (text == BTN_PENDING)   { await ShowPendingListAsync(bot, msg.Chat.Id, ct); return; }

            await bot.SendMessage(msg.Chat.Id, "Выберите действие:", replyMarkup: AdminMenu, cancellationToken: ct);
        }

        async Task HandleStartAsync(ITelegramBotClient bot, Message msg, bool isAdmin, CancellationToken ct)
        {
            long userId    = msg.From?.Id ?? 0;
            string firstName = msg.From?.FirstName ?? "";

            if (isAdmin)
            {
                await bot.SendMessage(msg.Chat.Id,
                    "👋  Привет, " + firstName + "!\n\nДобро пожаловать в панель управления Lampa.",
                    replyMarkup: AdminMenu, cancellationToken: ct);
                return;
            }

            var existing = _repo.GetById(userId.ToString());
            if (existing != null)
            {
                var exp = ParseExpiry(existing.Expires);
                if (exp < DateTime.UtcNow)
                {
                    bool hasPending = _pendingRepo.HasPendingFor(userId);
                    if (hasPending)
                    {
                        await bot.SendMessage(msg.Chat.Id,
                            "⏳  Заявка уже отправлена — ожидайте ответа.",
                            replyMarkup: UserMenu, cancellationToken: ct);
                    }
                    else
                    {
                        var kb = new InlineKeyboardMarkup(new[]
                        {
                            new[] { InlineKeyboardButton.WithCallbackData("🔄  Запросить продление", "req:" + userId) }
                        });
                        await bot.SendMessage(msg.Chat.Id,
                            "❌  Срок доступа истёк.\n\nВы можете запросить продление у администратора.",
                            replyMarkup: kb, cancellationToken: ct);
                    }
                }
                else
                {
                    int days = (int)(exp - DateTime.UtcNow).TotalDays;
                    await bot.SendMessage(msg.Chat.Id,
                        "👋  Привет, " + firstName + "!\n\n✅  Доступ активен\n📅  Действует до " +
                        exp.ToString("dd.MM.yyyy") + "  (" + days + " дн.)",
                        replyMarkup: UserMenu, cancellationToken: ct);
                }
                return;
            }

            // New unregistered user
            bool alreadyPending = _pendingRepo.HasPendingFor(userId);
            if (alreadyPending)
            {
                await bot.SendMessage(msg.Chat.Id,
                    "⏳  Заявка уже отправлена — ожидайте ответа.",
                    cancellationToken: ct);
                return;
            }

            var reqKb = new InlineKeyboardMarkup(new[]
            {
                new[] { InlineKeyboardButton.WithCallbackData("🚀  Запросить доступ", "req:" + userId) }
            });
            await bot.SendMessage(msg.Chat.Id,
                "👋  Привет, " + firstName + "!\n\n" +
                "<b>Lampa</b> — медиаплеер с открытым исходным кодом для просмотра фильмов и сериалов онлайн.\n\n" +
                "Нажмите кнопку, чтобы запросить доступ к сервису.",
                parseMode: ParseMode.Html,
                replyMarkup: reqKb, cancellationToken: ct);
        }

        async Task ShowUserProfileAsync(ITelegramBotClient bot, Message msg, CancellationToken ct)
        {
            long userId = msg.From?.Id ?? 0;
            var user = _repo.GetById(userId.ToString());
            if (user == null)
            {
                var kb = new InlineKeyboardMarkup(new[]
                {
                    new[] { InlineKeyboardButton.WithCallbackData("🚀  Запросить доступ", "req:" + userId) }
                });
                await bot.SendMessage(msg.Chat.Id, "❌  Вы не зарегистрированы.",
                    replyMarkup: kb, cancellationToken: ct);
                return;
            }

            var exp    = ParseExpiry(user.Expires);
            bool active = exp >= DateTime.UtcNow;
            int days   = (int)(exp - DateTime.UtcNow).TotalDays;

            await bot.SendMessage(msg.Chat.Id,
                "📋  <b>Мой профиль</b>\n\n" +
                "┌  <b>" + user.Comment + "</b>\n" +
                "│  🆔  <code>" + user.Id + "</code>\n" +
                "│  📅  до  <b>" + exp.ToString("dd.MM.yyyy") + "</b>\n" +
                "│  " + (active ? "✅  Активен · " + days + " дн." : "❌  Истёк") + "\n" +
                "└─────────────────",
                parseMode: ParseMode.Html,
                replyMarkup: UserMenu,
                cancellationToken: ct);
        }

        async Task HandleSupportAsync(ITelegramBotClient bot, Message msg, CancellationToken ct)
        {
            string firstName = msg.From?.FirstName ?? "";
            string username  = msg.From?.Username  ?? "";
            long userId      = msg.From?.Id ?? 0;

            string who = string.IsNullOrEmpty(username)
                ? firstName + "  (ID: " + userId + ")"
                : "@" + username + "  /  " + firstName + "  (ID: " + userId + ")";

            await bot.SendMessage(_conf.admin_id,
                "🆘  <b>Запрос поддержки</b>\n\n👤  " + who,
                parseMode: ParseMode.Html,
                replyMarkup: new InlineKeyboardMarkup(new[]
                {
                    new[] { InlineKeyboardButton.WithCallbackData("💬  Ответить", "support_reply:" + userId) }
                }),
                cancellationToken: ct);

            await bot.SendMessage(msg.Chat.Id,
                "🆘  Запрос отправлен администратору.\nОжидайте ответа.",
                replyMarkup: UserMenu, cancellationToken: ct);
        }

        async Task StartAddUserAsync(ITelegramBotClient bot, long chatId, long userId, CancellationToken ct)
        {
            SetState(userId, "add_id");
            await bot.SendMessage(chatId,
                "➕  <b>Добавить пользователя</b>\n\n<b>Шаг 1 из 3</b>  —  Введите Telegram ID\n\n/cancel — отмена",
                parseMode: ParseMode.Html, replyMarkup: AdminMenu, cancellationToken: ct);
        }

        async Task StartFindAsync(ITelegramBotClient bot, long chatId, long adminId, CancellationToken ct)
        {
            SetState(adminId, "find_query");
            await bot.SendMessage(chatId,
                "🔍  <b>Поиск пользователя</b>\n\nВведите Telegram ID, @username или часть имени:\n\n/cancel — отмена",
                parseMode: ParseMode.Html, replyMarkup: AdminMenu, cancellationToken: ct);
        }

        async Task StartBroadcastAsync(ITelegramBotClient bot, long chatId, long adminId, CancellationToken ct)
        {
            SetState(adminId, "broadcast_text");
            int active = _repo.ReadAll().Count(u => ParseExpiry(u.Expires) >= DateTime.UtcNow);
            await bot.SendMessage(chatId,
                "📢  <b>Рассылка</b>\n\nВведите текст сообщения.\nПолучателей (активных): <b>" + active + "</b>\n\n/cancel — отмена",
                parseMode: ParseMode.Html, replyMarkup: AdminMenu, cancellationToken: ct);
        }

        async Task ShowPendingListAsync(ITelegramBotClient bot, long chatId, CancellationToken ct)
        {
            var all = _pendingRepo.GetAll();
            if (all.Count == 0)
            {
                await bot.SendMessage(chatId, "📋  Нет активных заявок.", replyMarkup: AdminMenu, cancellationToken: ct);
                return;
            }

            var sb   = new StringBuilder();
            var rows = new List<InlineKeyboardButton[]>();
            sb.AppendLine("📋  <b>Активные заявки (" + all.Count + ")</b>\n");

            foreach (var (_, req) in all)
            {
                string displayName = string.IsNullOrEmpty(req.Username) ? req.FirstName : "@" + req.Username;
                string who = string.IsNullOrEmpty(req.Username)
                    ? req.FirstName + "  (ID: " + req.TelegramId + ")"
                    : "@" + req.Username + "  /  " + req.FirstName;
                sb.AppendLine("·  " + who + "  —  " + req.CreatedAt.ToLocalTime().ToString("dd.MM HH:mm"));
                string label = displayName.Length > 20 ? displayName.Substring(0, 17) + "…" : displayName;
                rows.Add(new[]
                {
                    InlineKeyboardButton.WithCallbackData("✅  " + label, "approve:" + req.TelegramId),
                    InlineKeyboardButton.WithCallbackData("❌  Отклонить",  "deny:"   + req.TelegramId)
                });
            }

            await bot.SendMessage(chatId, sb.ToString(),
                parseMode: ParseMode.Html,
                replyMarkup: new InlineKeyboardMarkup(rows),
                cancellationToken: ct);
        }

        async Task HandleAdminInputAsync(ITelegramBotClient bot, long chatId, long userId, string state, string input, CancellationToken ct)
        {
            if (input == "/cancel" ||
                input == BTN_USERS || input == BTN_ADD  || input == BTN_STATS ||
                input == BTN_FIND  || input == BTN_BROADCAST || input == BTN_PENDING)
            {
                ClearState(userId);
                if (input == BTN_USERS)     { await ShowUserListAsync(bot, chatId, 0, ct); return; }
                if (input == BTN_STATS)     { await ShowStatsAsync(bot, chatId, ct); return; }
                if (input == BTN_ADD)       { await StartAddUserAsync(bot, chatId, userId, ct); return; }
                if (input == BTN_FIND)      { await StartFindAsync(bot, chatId, userId, ct); return; }
                if (input == BTN_BROADCAST) { await StartBroadcastAsync(bot, chatId, userId, ct); return; }
                if (input == BTN_PENDING)   { await ShowPendingListAsync(bot, chatId, ct); return; }
                await bot.SendMessage(chatId, "↩️  Отменено.", replyMarkup: AdminMenu, cancellationToken: ct);
                return;
            }

            // ── Search ──────────────────────────────────────────────
            if (state == "find_query")
            {
                ClearState(userId);
                var found = _repo.Find(input);
                if (found == null)
                {
                    await bot.SendMessage(chatId,
                        "🔍  По запросу «" + input + "» ничего не найдено.",
                        replyMarkup: AdminMenu, cancellationToken: ct);
                    return;
                }
                await ShowUserCardAsync(bot, chatId, found.Id, ct);
                return;
            }

            // ── Broadcast ────────────────────────────────────────────
            if (state == "broadcast_text")
            {
                int active = _repo.ReadAll().Count(u => ParseExpiry(u.Expires) >= DateTime.UtcNow);
                SetState(userId, "broadcast_confirm:" + input);
                await bot.SendMessage(chatId,
                    "📢  <b>Предпросмотр рассылки</b>\n\nПолучателей: <b>" + active + "</b>\n\n" +
                    "──────────────────\n" + input + "\n──────────────────",
                    parseMode: ParseMode.Html,
                    replyMarkup: new InlineKeyboardMarkup(new[]
                    {
                        new[]
                        {
                            InlineKeyboardButton.WithCallbackData("✅  Отправить", "bc_confirm"),
                            InlineKeyboardButton.WithCallbackData("❌  Отмена",    "bc_cancel")
                        }
                    }),
                    cancellationToken: ct);
                return;
            }

            if (state.StartsWith("broadcast_confirm:"))
            {
                await bot.SendMessage(chatId,
                    "Нажмите <b>✅  Отправить</b> под сообщением выше или /cancel.",
                    parseMode: ParseMode.Html, replyMarkup: AdminMenu, cancellationToken: ct);
                return;
            }

            // ── Add user ─────────────────────────────────────────────
            if (state == "add_id")
            {
                if (!long.TryParse(input, out _))
                {
                    await bot.SendMessage(chatId,
                        "⚠️  Telegram ID — это число.\nПопробуйте ещё раз или /cancel",
                        replyMarkup: AdminMenu, cancellationToken: ct);
                    return;
                }
                if (_repo.Exists(input))
                {
                    await bot.SendMessage(chatId,
                        "⚠️  Пользователь <code>" + input + "</code> уже существует.\nВведите другой ID или /cancel",
                        parseMode: ParseMode.Html, replyMarkup: AdminMenu, cancellationToken: ct);
                    return;
                }
                SetState(userId, "add_name:" + input);
                await bot.SendMessage(chatId,
                    "➕  <b>Добавить пользователя</b>\n\n<b>Шаг 2 из 3</b>  —  Введите имя для <code>" + input + "</code>\n" +
                    "(например: @username или Иван Иванов)\n\n/cancel — отмена",
                    parseMode: ParseMode.Html, replyMarkup: AdminMenu, cancellationToken: ct);
                return;
            }

            if (state.StartsWith("add_name:"))
            {
                var newId = state.Substring("add_name:".Length);
                var exp   = DateTime.Now.AddDays(_conf.default_expire_days);
                // Store name in state to avoid Telegram's 64-byte callback data limit
                SetState(userId, "confirm_add:" + newId + ":" + input);
                var kb = new InlineKeyboardMarkup(new[]
                {
                    new[] { InlineKeyboardButton.WithCallbackData("✅  Подтвердить", "addc_confirm") }
                });
                await bot.SendMessage(chatId,
                    "➕  <b>Добавить пользователя</b>\n\n<b>Шаг 3 из 3</b>  —  Подтвердите данные\n\n" +
                    "👤  <b>" + input + "</b>\n" +
                    "🆔  <code>" + newId + "</code>\n" +
                    "📅  Доступ до: <b>" + exp.ToString("dd.MM.yyyy") + "</b>",
                    parseMode: ParseMode.Html, replyMarkup: kb, cancellationToken: ct);
                return;
            }

            if (state.StartsWith("confirm_add:"))
            {
                await bot.SendMessage(chatId,
                    "Нажмите <b>✅  Подтвердить</b> под сообщением выше или /cancel.",
                    parseMode: ParseMode.Html, replyMarkup: AdminMenu, cancellationToken: ct);
                return;
            }

            // ── Support reply ─────────────────────────────────────────
            if (state.StartsWith("support_reply:"))
            {
                long targetUid = long.Parse(state.Substring("support_reply:".Length));
                ClearState(userId);
                try
                {
                    await bot.SendMessage(targetUid,
                        "📨  <b>Сообщение от администратора:</b>\n\n" + input,
                        parseMode: ParseMode.Html, replyMarkup: UserMenu, cancellationToken: ct);
                    await bot.SendMessage(chatId,
                        "✅  Отправлено пользователю <code>" + targetUid + "</code>.",
                        parseMode: ParseMode.Html, replyMarkup: AdminMenu, cancellationToken: ct);
                }
                catch
                {
                    await bot.SendMessage(chatId,
                        "❌  Не удалось отправить — пользователь заблокировал бота.",
                        replyMarkup: AdminMenu, cancellationToken: ct);
                }
                return;
            }

            // ── Set expiry date ───────────────────────────────────────
            if (state.StartsWith("setexpire_date:"))
            {
                var targetId = state.Substring("setexpire_date:".Length);
                if (!DateTime.TryParse(input, out var newDate))
                {
                    await bot.SendMessage(chatId,
                        "⚠️  Неверный формат. Введите дату: <b>ГГГГ-ММ-ДД</b>\nНапример: 2027-01-01\n\n/cancel — отмена",
                        parseMode: ParseMode.Html, replyMarkup: AdminMenu, cancellationToken: ct);
                    return;
                }
                ClearState(userId);
                var users = _repo.ReadAll();
                var idx = users.FindIndex(u => u.Id == targetId);
                if (idx < 0)
                {
                    await bot.SendMessage(chatId, "❌  Пользователь не найден.", replyMarkup: AdminMenu, cancellationToken: ct);
                    return;
                }
                users[idx].Expires = StoreExpiry(newDate);
                _repo.WriteAll(users);
                await bot.SendMessage(chatId,
                    "✅  Дата обновлена → <b>" + newDate.ToString("dd.MM.yyyy") + "</b>",
                    parseMode: ParseMode.Html, replyMarkup: AdminMenu, cancellationToken: ct);
                await ShowUserCardAsync(bot, chatId, targetId, ct);
            }
        }

        #endregion

        #region User list

        async Task ShowUserListAsync(ITelegramBotClient bot, long chatId, int page, CancellationToken ct)
        {
            const int pageSize = 5;
            var users = _repo.ReadAll();
            if (users.Count == 0)
            {
                await bot.SendMessage(chatId, "📭  Список пуст.", replyMarkup: AdminMenu, cancellationToken: ct);
                return;
            }

            var paged      = users.Skip(page * pageSize).Take(pageSize).ToList();
            int totalPages = (int)Math.Ceiling(users.Count / (double)pageSize);
            var rows       = new List<InlineKeyboardButton[]>();

            foreach (var u in paged)
            {
                var exp    = ParseExpiry(u.Expires);
                bool active = exp >= DateTime.UtcNow;
                string icon = active ? "✅" : "❌";
                string name = u.Comment.Length > 35 ? u.Comment.Substring(0, 32) + "…" : u.Comment;
                rows.Add(new[] { InlineKeyboardButton.WithCallbackData(icon + "  " + name, "user:" + u.Id) });
            }

            if (totalPages > 1)
            {
                var nav = new List<InlineKeyboardButton>();
                if (page > 0)               nav.Add(InlineKeyboardButton.WithCallbackData("◀", "list:" + (page - 1)));
                nav.Add(InlineKeyboardButton.WithCallbackData((page + 1) + " / " + totalPages, "noop"));
                if (page < totalPages - 1)  nav.Add(InlineKeyboardButton.WithCallbackData("▶", "list:" + (page + 1)));
                rows.Add(nav.ToArray());
            }

            int activeCount = users.Count(u => ParseExpiry(u.Expires) >= DateTime.UtcNow);
            await bot.SendMessage(chatId,
                "👥  <b>Пользователи</b>\n<i>Всего: " + users.Count + "  ·  Активных: " + activeCount +
                "  ·  Истёкших: " + (users.Count - activeCount) + "</i>",
                parseMode: ParseMode.Html,
                replyMarkup: new InlineKeyboardMarkup(rows),
                cancellationToken: ct);
        }

        #endregion

        #region User card

        async Task ShowUserCardAsync(ITelegramBotClient bot, long chatId, string userId, CancellationToken ct)
        {
            var user = _repo.GetById(userId);
            if (user == null) { await bot.SendMessage(chatId, "❌  Не найден.", replyMarkup: AdminMenu, cancellationToken: ct); return; }
            await bot.SendMessage(chatId, BuildCardText(user), parseMode: ParseMode.Html, replyMarkup: BuildCardKb(user), cancellationToken: ct);
        }

        async Task UpdateCardAsync(ITelegramBotClient bot, long chatId, int msgId, string userId, CancellationToken ct)
        {
            var user = _repo.GetById(userId);
            if (user == null) return;
            try { await bot.EditMessageText(chatId, msgId, BuildCardText(user), parseMode: ParseMode.Html, replyMarkup: BuildCardKb(user), cancellationToken: ct); } catch { }
        }

        static string BuildCardText(LampacUser u)
        {
            var exp     = ParseExpiry(u.Expires);
            bool active  = exp >= DateTime.UtcNow;
            int daysLeft = (int)(exp - DateTime.UtcNow).TotalDays;
            string status = active ? "✅  Активен · " + daysLeft + " дн." : "❌  Истёк";
            return
                "┌  <b>" + u.Comment + "</b>\n" +
                "│  🆔  <code>" + u.Id + "</code>\n" +
                "│  📅  до  <b>" + exp.ToString("dd.MM.yyyy") + "</b>\n" +
                "│  " + status + "\n" +
                "└─────────────────";
        }

        static InlineKeyboardMarkup BuildCardKb(LampacUser u)
        {
            var exp    = ParseExpiry(u.Expires);
            bool active = exp >= DateTime.UtcNow;
            var rows   = new List<InlineKeyboardButton[]>();
            rows.Add(active
                ? new[] { InlineKeyboardButton.WithCallbackData("❌  Заблокировать",  "block:"   + u.Id) }
                : new[] { InlineKeyboardButton.WithCallbackData("✅  Разблокировать", "unblock:" + u.Id) });
            rows.Add(new[]
            {
                InlineKeyboardButton.WithCallbackData("📅  Продлить", "setexpire:" + u.Id),
                InlineKeyboardButton.WithCallbackData("🗑  Удалить",  "del:"       + u.Id)
            });
            rows.Add(new[] { InlineKeyboardButton.WithCallbackData("‹  Назад", "list:0") });
            return new InlineKeyboardMarkup(rows);
        }

        #endregion

        #region Stats

        async Task ShowStatsAsync(ITelegramBotClient bot, long chatId, CancellationToken ct)
        {
            var users  = _repo.ReadAll();
            int total  = users.Count;
            int active = users.Count(u => ParseExpiry(u.Expires) >= DateTime.UtcNow);

            var expiring = users
                .Where(u => ParseExpiry(u.Expires) >= DateTime.UtcNow)
                .OrderBy(u => ParseExpiry(u.Expires))
                .Take(3).ToList();

            var sb = new StringBuilder();
            sb.AppendLine("📊  <b>Статистика</b>\n");
            sb.AppendLine("👥  Всего: <b>" + total + "</b>");
            sb.AppendLine("✅  Активных: <b>" + active + "</b>");
            sb.AppendLine("❌  Истёкших: <b>" + (total - active) + "</b>");
            sb.AppendLine("📋  Заявок в очереди: <b>" + _pendingRepo.Count + "</b>");

            if (expiring.Count > 0)
            {
                sb.AppendLine("\n⏰  <b>Истекают скоро</b>");
                foreach (var u in expiring)
                {
                    var exp  = ParseExpiry(u.Expires);
                    int days = (int)(exp - DateTime.UtcNow).TotalDays;
                    sb.AppendLine("·  " + u.Comment + "  —  " + days + " дн.");
                }
            }

            await bot.SendMessage(chatId, sb.ToString(),
                parseMode: ParseMode.Html, replyMarkup: AdminMenu, cancellationToken: ct);
        }

        #endregion

        #region Daily notifications

        /// <summary>
        /// Called every hour by the hosted service.
        /// Runs at most once per UTC calendar day: notifies users expiring in ≤7 days
        /// and sends an admin summary.
        /// </summary>
        public async Task RunDailyNotificationsAsync(ITelegramBotClient bot, CancellationToken ct)
        {
            var today = DateTime.UtcNow.Date.ToString("yyyy-MM-dd");
            try
            {
                if (File.Exists(_notifPath) && File.ReadAllText(_notifPath).Trim() == today) return;
            }
            catch { }

            var users = _repo.ReadAll();
            var expiringSoon = users
                .Select(u => (User: u, Exp: ParseExpiry(u.Expires)))
                .Where(x => x.Exp > DateTime.UtcNow && x.Exp <= DateTime.UtcNow.AddDays(7))
                .OrderBy(x => x.Exp)
                .ToList();

            // Mark as run even if nothing to send, so we don't check again until tomorrow
            try { File.WriteAllText(_notifPath, today); } catch { }

            if (expiringSoon.Count == 0) return;

            foreach (var (user, exp) in expiringSoon)
            {
                if (!long.TryParse(user.Id, out long uid)) continue;
                int days = Math.Max(1, (int)(exp - DateTime.UtcNow).TotalDays);
                try
                {
                    await bot.SendMessage(uid,
                        "⏰  Срок доступа истекает через <b>" + days + " дн.</b>  (" + exp.ToString("dd.MM.yyyy") + ")\n\n" +
                        "Обратитесь к администратору для продления.",
                        parseMode: ParseMode.Html,
                        replyMarkup: UserMenu,
                        cancellationToken: ct);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("[TelegramBot] Не удалось уведомить пользователя {Uid}: {Msg}", uid, ex.Message);
                }
            }

            var sbAdmin = new StringBuilder();
            sbAdmin.AppendLine("⏰  <b>Истекают в течение 7 дней</b>\n");
            foreach (var (user, exp) in expiringSoon)
            {
                int days = Math.Max(1, (int)(exp - DateTime.UtcNow).TotalDays);
                sbAdmin.AppendLine("·  " + user.Comment + "  —  " + days + " дн.  (" + exp.ToString("dd.MM.yyyy") + ")");
            }
            try
            {
                await bot.SendMessage(_conf.admin_id, sbAdmin.ToString(),
                    parseMode: ParseMode.Html, cancellationToken: ct);
            }
            catch (Exception ex)
            {
                _logger.LogError("[TelegramBot] Не удалось отправить сводку админу: {Msg}", ex.Message);
            }
        }

        #endregion

        #region Callbacks

        async Task HandleCallbackAsync(ITelegramBotClient bot, CallbackQuery cb, CancellationToken ct)
        {
            var  data     = cb.Data ?? "";
            long callerId = cb.From.Id;
            long chatId   = cb.Message?.Chat.Id ?? 0;
            int  msgId    = cb.Message?.MessageId ?? 0;

            // ── Access request (new or expired user) ─────────────────
            if (data.StartsWith("req:"))
            {
                long rid = long.Parse(data.Substring(4));
                if (callerId != rid)
                {
                    await bot.AnswerCallbackQuery(cb.Id, "Это не ваша кнопка.", showAlert: true, cancellationToken: ct);
                    return;
                }

                var existingUser = _repo.GetById(rid.ToString());
                if (existingUser != null && ParseExpiry(existingUser.Expires) >= DateTime.UtcNow)
                {
                    await bot.AnswerCallbackQuery(cb.Id, cancellationToken: ct);
                    await bot.EditMessageText(chatId, msgId, "✅  Доступ уже активен.", cancellationToken: ct);
                    return;
                }

                if (_pendingRepo.HasPendingFor(rid))
                {
                    await bot.AnswerCallbackQuery(cb.Id, "Заявка уже отправлена.", showAlert: true, cancellationToken: ct);
                    return;
                }

                var req = new PendingRequest
                {
                    TelegramId = rid,
                    Username   = cb.From.Username ?? "",
                    FirstName  = cb.From.FirstName ?? ""
                };
                bool isRenewal = existingUser != null;
                string who = string.IsNullOrEmpty(req.Username)
                    ? req.FirstName + "  (ID: " + rid + ")"
                    : "@" + req.Username + "  /  " + req.FirstName + "  (ID: " + rid + ")";

                string adminText = isRenewal
                    ? "🔄  <b>Запрос на продление</b>\n\n👤  " + who + "\n🕐  " + DateTime.Now.ToString("dd.MM.yyyy  HH:mm")
                    : "📬  <b>Новая заявка на доступ</b>\n\n👤  " + who + "\n🕐  " + DateTime.Now.ToString("dd.MM.yyyy  HH:mm");

                var adminKb = new InlineKeyboardMarkup(new[]
                {
                    new[]
                    {
                        InlineKeyboardButton.WithCallbackData("✅  Одобрить",  "approve:" + rid),
                        InlineKeyboardButton.WithCallbackData("❌  Отклонить", "deny:"    + rid)
                    }
                });
                var aMsg = await bot.SendMessage(_conf.admin_id,
                    adminText, parseMode: ParseMode.Html, replyMarkup: adminKb, cancellationToken: ct);
                _pendingRepo.Add(aMsg.MessageId, req);
                await bot.AnswerCallbackQuery(cb.Id, cancellationToken: ct);
                await bot.EditMessageText(chatId, msgId, "⏳  Заявка отправлена — ожидайте ответа.", cancellationToken: ct);
                return;
            }

            // ── Admin-only from here ──────────────────────────────────
            if (callerId != _conf.admin_id)
            {
                await bot.AnswerCallbackQuery(cb.Id, "Нет доступа.", showAlert: true, cancellationToken: ct);
                return;
            }

            if (data == "noop") { await bot.AnswerCallbackQuery(cb.Id, cancellationToken: ct); return; }

            if (data.StartsWith("list:"))
            {
                int pg = int.Parse(data.Substring(5));
                await bot.AnswerCallbackQuery(cb.Id, cancellationToken: ct);
                try { await bot.DeleteMessage(chatId, msgId, cancellationToken: ct); } catch { }
                await ShowUserListAsync(bot, chatId, pg, ct);
                return;
            }

            if (data.StartsWith("user:"))
            {
                string uid = data.Substring(5);
                await bot.AnswerCallbackQuery(cb.Id, cancellationToken: ct);
                try { await bot.DeleteMessage(chatId, msgId, cancellationToken: ct); } catch { }
                await ShowUserCardAsync(bot, chatId, uid, ct);
                return;
            }

            if (data.StartsWith("block:"))
            {
                string uid = data.Substring(6);
                var ul = _repo.ReadAll(); var i = ul.FindIndex(u => u.Id == uid);
                if (i >= 0) { ul[i].Expires = StoreExpiry(DateTime.Now.AddYears(-1)); _repo.WriteAll(ul); }
                await bot.AnswerCallbackQuery(cb.Id, "🔒  Заблокирован", cancellationToken: ct);
                await UpdateCardAsync(bot, chatId, msgId, uid, ct);
                return;
            }

            if (data.StartsWith("unblock:"))
            {
                string uid = data.Substring(8);
                var ul = _repo.ReadAll(); var i = ul.FindIndex(u => u.Id == uid);
                if (i >= 0) { ul[i].Expires = StoreExpiry(DateTime.Now.AddDays(_conf.default_expire_days)); _repo.WriteAll(ul); }
                await bot.AnswerCallbackQuery(cb.Id, "🔓  Разблокирован", cancellationToken: ct);
                await UpdateCardAsync(bot, chatId, msgId, uid, ct);
                return;
            }

            if (data.StartsWith("setexpire:"))
            {
                string uid = data.Substring("setexpire:".Length);
                SetState(callerId, "setexpire_date:" + uid);
                await bot.AnswerCallbackQuery(cb.Id, cancellationToken: ct);
                await bot.SendMessage(chatId,
                    "📅  Введите новую дату\nФормат: <b>ГГГГ-ММ-ДД</b>  (например: 2027-01-01)\n\n/cancel — отмена",
                    parseMode: ParseMode.Html, replyMarkup: AdminMenu, cancellationToken: ct);
                return;
            }

            if (data.StartsWith("del:"))
            {
                string uid  = data.Substring(4);
                var ul   = _repo.ReadAll();
                var user = ul.FirstOrDefault(u => u.Id == uid);
                if (user != null)
                {
                    ul.Remove(user);
                    _repo.WriteAll(ul);
                    await bot.AnswerCallbackQuery(cb.Id, "🗑  Удалён", cancellationToken: ct);
                    await bot.EditMessageText(chatId, msgId,
                        "🗑  <b>" + user.Comment + "</b>  удалён.",
                        parseMode: ParseMode.Html,
                        replyMarkup: new InlineKeyboardMarkup(new[]
                        {
                            new[] { InlineKeyboardButton.WithCallbackData("‹  К списку", "list:0") }
                        }),
                        cancellationToken: ct);
                }
                return;
            }

            // ── Confirm add user ──────────────────────────────────────
            if (data == "addc_confirm")
            {
                var confirmState = GetState(callerId);
                if (confirmState?.StartsWith("confirm_add:") != true)
                {
                    await bot.AnswerCallbackQuery(cb.Id, "Устарело. Начните заново.", showAlert: true, cancellationToken: ct);
                    return;
                }
                var remainder = confirmState.Substring("confirm_add:".Length);
                int sep    = remainder.IndexOf(':');
                string nid     = sep >= 0 ? remainder.Substring(0, sep)  : remainder;
                string comment = sep >= 0 ? remainder.Substring(sep + 1) : "";
                ClearState(callerId);

                if (_repo.Exists(nid))
                {
                    await bot.AnswerCallbackQuery(cb.Id, "Уже существует!", showAlert: true, cancellationToken: ct);
                    return;
                }
                var exp = DateTime.Now.AddDays(_conf.default_expire_days);
                _repo.AddOrReplace(new LampacUser
                {
                    Id      = nid, Group = 1,
                    Expires = StoreExpiry(exp),
                    Comment = comment,
                    Params  = new LampacUserParams { Adult = false, Admin = false }
                });
                await bot.AnswerCallbackQuery(cb.Id, "✅  Добавлен", cancellationToken: ct);
                await bot.EditMessageText(chatId, msgId,
                    "✅  <b>Пользователь добавлен</b>\n\n" +
                    "┌  <b>" + comment + "</b>\n" +
                    "│  🆔  <code>" + nid + "</code>\n" +
                    "│  📅  до  <b>" + exp.ToString("dd.MM.yyyy") + "</b>\n" +
                    "└─────────────────",
                    parseMode: ParseMode.Html,
                    replyMarkup: new InlineKeyboardMarkup(new[]
                    {
                        new[] { InlineKeyboardButton.WithCallbackData("👤  Открыть карточку", "user:" + nid) },
                        new[] { InlineKeyboardButton.WithCallbackData("‹  К списку",          "list:0") }
                    }),
                    cancellationToken: ct);
                if (long.TryParse(nid, out long tgId))
                    try { await bot.SendMessage(tgId, "🎉  Вам выдан доступ к Lampa!\n📅  Действует до " + exp.ToString("dd.MM.yyyy"), replyMarkup: UserMenu, cancellationToken: ct); } catch { }
                return;
            }

            // ── Broadcast confirm / cancel ────────────────────────────
            if (data == "bc_confirm")
            {
                var confirmState = GetState(callerId);
                if (confirmState?.StartsWith("broadcast_confirm:") != true)
                {
                    await bot.AnswerCallbackQuery(cb.Id, "Устарело. Начните заново.", showAlert: true, cancellationToken: ct);
                    return;
                }
                var message = confirmState.Substring("broadcast_confirm:".Length);
                ClearState(callerId);

                var activeUsers = _repo.ReadAll()
                    .Where(u => ParseExpiry(u.Expires) >= DateTime.UtcNow)
                    .ToList();

                int sent = 0, failed = 0;
                foreach (var u in activeUsers)
                {
                    if (!long.TryParse(u.Id, out long uid)) continue;
                    try   { await bot.SendMessage(uid, "📢  " + message, cancellationToken: ct); sent++; }
                    catch { failed++; }
                }

                await bot.AnswerCallbackQuery(cb.Id, "✅  Отправлено", cancellationToken: ct);
                await bot.EditMessageText(chatId, msgId,
                    "📢  <b>Рассылка завершена</b>\n\n✅  Доставлено: <b>" + sent + "</b>\n❌  Не доставлено: <b>" + failed + "</b>",
                    parseMode: ParseMode.Html, cancellationToken: ct);
                return;
            }

            if (data == "bc_cancel")
            {
                ClearState(callerId);
                await bot.AnswerCallbackQuery(cb.Id, "Отменено", cancellationToken: ct);
                await bot.EditMessageText(chatId, msgId, "❌  Рассылка отменена.", cancellationToken: ct);
                return;
            }

            // ── Approve / deny ────────────────────────────────────────
            if (data.StartsWith("approve:"))
            {
                long uid = long.Parse(data.Substring("approve:".Length));
                // TryGetAndRemove checks by msgId first, then falls back to userId
                // (covers both: original notification message and pending-list view)
                _pendingRepo.TryGetAndRemove(msgId, uid, out var req);
                req ??= new PendingRequest { TelegramId = uid };
                var exp     = DateTime.Now.AddDays(_conf.default_expire_days);
                string comment = BuildComment(req);
                _repo.AddOrReplace(new LampacUser
                {
                    Id      = uid.ToString(), Group = 1,
                    Expires = StoreExpiry(exp),
                    Comment = comment,
                    Params  = new LampacUserParams { Adult = false, Admin = false }
                });
                await bot.AnswerCallbackQuery(cb.Id, "✅  Одобрено", cancellationToken: ct);
                await bot.EditMessageText(chatId, msgId,
                    "✅  <b>Одобрено</b>: " + comment + "\n📅  до " + exp.ToString("dd.MM.yyyy"),
                    parseMode: ParseMode.Html, cancellationToken: ct);
                try { await bot.SendMessage(uid, "🎉  Доступ одобрен!\n📅  Действует до " + exp.ToString("dd.MM.yyyy"), replyMarkup: UserMenu, cancellationToken: ct); } catch { }
                return;
            }

            if (data.StartsWith("deny:"))
            {
                long uid = long.Parse(data.Substring(5));
                _pendingRepo.TryGetAndRemove(msgId, uid, out var req);
                string name = req != null ? BuildComment(req) : uid.ToString();
                await bot.AnswerCallbackQuery(cb.Id, "❌  Отклонено", cancellationToken: ct);
                await bot.EditMessageText(chatId, msgId,
                    "❌  <b>Отклонено</b>: " + name, parseMode: ParseMode.Html, cancellationToken: ct);
                try { await bot.SendMessage(uid, "❌  Ваша заявка отклонена.", cancellationToken: ct); } catch { }
                return;
            }

            if (data.StartsWith("support_reply:"))
            {
                long uid = long.Parse(data.Substring("support_reply:".Length));
                await bot.AnswerCallbackQuery(cb.Id, cancellationToken: ct);
                await bot.SendMessage(chatId,
                    "💬  Введите сообщение для <code>" + uid + "</code>:\n\n/cancel — отмена",
                    parseMode: ParseMode.Html, replyMarkup: AdminMenu, cancellationToken: ct);
                SetState(callerId, "support_reply:" + uid);
                return;
            }
        }

        #endregion

        static string BuildComment(PendingRequest r)
        {
            if (!string.IsNullOrEmpty(r.Username))
                return ("@" + r.Username + " / " + r.FirstName).Trim(' ', '/');
            return r.FirstName.Trim();
        }
    }
}
