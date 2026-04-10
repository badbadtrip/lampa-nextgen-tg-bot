using System;
using Microsoft.Extensions.DependencyInjection;
using Shared.Models.Events;
using Shared.Models.Module;
using Shared.Models.Module.Interfaces;
using Shared.Services;
using TelegramBot.Models;
using TelegramBot.Services;

namespace TelegramBot
{
    public class ModInit : IModuleLoaded, IModuleConfigure
    {
        public static TelegramBotConf conf = new();

        public void Configure(ConfigureModel app)
        {
            SyncConf();
            app.services.AddHostedService<TelegramBotHostedService>();
        }

        public void Loaded(InitspaceModel initspace)
        {
            SyncConf();
            EventListener.UpdateInitFile += SyncConf;
        }

        public void Dispose()
        {
            EventListener.UpdateInitFile -= SyncConf;
        }

        static void SyncConf()
        {
            conf = ModuleInvoke.Init("TelegramBot", DefaultConf());

            if (conf.enable && string.IsNullOrWhiteSpace(conf.bot_token))
                Console.WriteLine("[TelegramBot] enable=true, но bot_token пустой — проверьте секцию TelegramBot в init.conf.");

            if (conf.enable && conf.admin_id == 0)
                Console.WriteLine("[TelegramBot] enable=true, но admin_id не задан — проверьте секцию TelegramBot в init.conf.");
        }

        static TelegramBotConf DefaultConf() => new()
        {
            enable             = true,
            bot_token          = "",
            admin_id           = 0,
            users_file_path    = "users.json",
            default_expire_days = 365
        };
    }
}
