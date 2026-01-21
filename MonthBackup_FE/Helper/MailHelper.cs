using System;
using System.Net;
using System.Net.Mail;
using System.Configuration;
using System.Net.Configuration;

namespace MonthBackup_FE.Helper
{
    public static class MailHelper
    {
        /// <summary>
        /// 根據 app.config/web.config 的 <system.net><mailSettings> 設定寄信
        /// </summary>
        /// <param name="subject">信件主旨</param>
        /// <param name="body">信件內容</param>
        /// <param name="to">收件人（單一位址）</param>
        public static void SendMail(string subject, string body, string to)
        {
            try
            {
                // 讀取組態中預設的 smtp 設定
                SmtpSection smtpSection = (SmtpSection)ConfigurationManager.GetSection("system.net/mailSettings/smtp");
                if (smtpSection == null)
                {
                    throw new InvalidOperationException("找不到 mailSettings/smtp 設定，請確認組態檔。");
                }

                string host = smtpSection.Network.Host;
                int port = smtpSection.Network.Port;
                string userName = smtpSection.Network.UserName;
                string password = smtpSection.Network.Password;

                using (SmtpClient client = new SmtpClient())
                {
                    client.Host = host;
                    client.Port = port;
                    client.EnableSsl = false; // 依你實際需求調整，port 25 通常不啟用 SSL
                    client.Credentials = new NetworkCredential(userName, password);
                    client.DeliveryMethod = SmtpDeliveryMethod.Network;

                    using (MailMessage message = new MailMessage())
                    {
                        // 寄件人用組態中的 userName
                        message.From = new MailAddress(userName + "@chipmos.com.tw");
                        message.To.Add(to);
                        message.Subject = subject;
                        message.Body = body;
                        message.IsBodyHtml = false;

                        client.Send(message);
                    }
                }
            }
            catch (Exception ex)
            {
                // 寄信失敗不要讓主流程掛掉，寫 log 即可
                LogHelper.WriteLog("MAIL", "寄信失敗: " + ex.Message);
            }
        }
    }
}