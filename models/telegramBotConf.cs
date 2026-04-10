namespace TelegramBot.Models
{
    public class TelegramBotConf
    {
        public bool enable { get; set; } = true;

        public string bot_token { get; set; } = "";

        public long admin_id { get; set; } = 0;

        public string users_file_path { get; set; } = "users.json";

        public int default_expire_days { get; set; } = 365;
    }
}
