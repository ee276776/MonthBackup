using CPISData.Data;
using System;

namespace MonthBackup_FE.AR_EXTEND.Provider
{
    /// <summary>
    /// AR_EXTEND 資料處理提供者：對應 ar_extend.4gl 的 GLOBAL g 與 getARDate()
    /// </summary>
    public static class ArExtendProvider
    {
        // 資料庫連線字串
        public static string ConnectionString = ConnectionStrings.FromName("comets");

        /// <summary>
        /// 4GL 的 g.end_date (arg_val(1))，在原程式只當參數記錄
        /// </summary>
        public static string EndDateRaw { get; set; }

        /// <summary>
        /// 4GL 的 g.flag：UNLOAD 或 DELETE
        /// </summary>
        public static string Flag { get; set; }

        /// <summary>
        /// 4GL 的 g.file_name：LOAD FROM 這個檔案進 ar_temp
        /// </summary>
        public static string LotFileName { get; set; }

        /// <summary>
        /// 對應 4GL FUNCTION getARDate(pa_keep_month)
        /// 根據 keep_month 算出 AR 截止日 (該月前一日)
        /// </summary>
        public static DateTime GetArDate(string keepMonth)
        {
            if (string.IsNullOrEmpty(keepMonth))
            {
                throw new ArgumentException("keep_month 不可為空", "keepMonth");
            }

            int keep;
            if (!int.TryParse(keepMonth, out keep))
            {
                throw new ArgumentException("keep_month 格式錯誤: " + keepMonth, "keepMonth");
            }

            DateTime today = DateTime.Today;
            int year = today.Year;
            int month = today.Month;

            // pa_first_date = 該月 1 號
            DateTime firstDateOfThisMonth = new DateTime(year, month, 1);

            // pa_ar_date = pa_first_date - INTERVAL(keep_month) MONTH
            DateTime arDate = firstDateOfThisMonth.AddMonths(-keep);

            // MDY(MONTH(pa_ar_date), 1, YEAR(pa_ar_date))
            arDate = new DateTime(arDate.Year, arDate.Month, 1);

            // pa_ar_date = pa_ar_date - 1
            arDate = arDate.AddDays(-1);

            return arDate;
        }

        /// <summary>
        /// 將 DateTime 轉成 yyyymmdd (int)
        /// </summary>
        public static int ToIntYyyymmdd(DateTime date)
        {
            return date.Year * 10000 + date.Month * 100 + date.Day;
        }

        /// <summary>
        /// 將 yyyymmdd int 轉成 8 碼字串
        /// </summary>
        public static string ToStringYyyymmdd(int yyyymmdd)
        {
            return yyyymmdd.ToString("D8");
        }
    }
}