using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MonthBackup_FE.PRODASSYS_SPC_AR_DEL.Provider
{
    /// <summary>
    /// 對應 prodassys_spc_ar_del.4gl GLOBAL & 日期計算
    /// </summary>
    public static class ProdassysSpcArDelProvider
    {
        public static string RunDateRaw { get; set; }
        public static int FromDateInt { get; set; }
        public static int ToDateInt { get; set; }
        public static DateTime VYyyymm { get; set; }
        public static DateTime V6mFrom { get; set; }
        public static DateTime V6mTo { get; set; }

        /// <summary>
        /// 根據 4GL MAIN 的邏輯計算 v_yyyymm, v_6m_from, v_6m_to, v_from_date, v_to_date
        /// 支援輸入格式:
        ///  - 空字串 → 用今天 yyyy-MM
        ///  - yyyyMM / yyyy-MM
        ///  - yyyyMMdd
        /// 最後都會變成 yyyy-MM。
        /// </summary>
        public static void InitDates(string runDateRaw)
        {
            if (string.IsNullOrEmpty(runDateRaw))
            {
                RunDateRaw = DateTime.Today.ToString("yyyy-MM");
            }
            else
            {
                string s = runDateRaw.Trim();
                string digitsOnly = s.Replace("-", "").Replace("/", "");

                if (digitsOnly.Length == 6)
                {
                    // yyyyMM → yyyy-MM
                    string yyyy = digitsOnly.Substring(0, 4);
                    string mm = digitsOnly.Substring(4, 2);
                    RunDateRaw = yyyy + "-" + mm;
                }
                else if (digitsOnly.Length == 8)
                {
                    // yyyyMMdd → yyyy-MM
                    string yyyy = digitsOnly.Substring(0, 4);
                    string mm = digitsOnly.Substring(4, 2);
                    RunDateRaw = yyyy + "-" + mm;
                }
                else if (s.Length == 7 && s[4] == '-')
                {
                    RunDateRaw = s;
                }
                else
                {
                    throw new ArgumentException("v_run_date 格式不正確，請輸入 yyyyMM / yyyy-MM / yyyyMMdd", "runDateRaw");
                }
            }

            // v_yyyymm = v_run_date (datetime year to month)，用當月 1 號代表
            DateTime yyyymm;
            if (!DateTime.TryParseExact(RunDateRaw + "-01", "yyyy-MM-dd",
                    System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.None,
                    out yyyymm))
            {
                throw new ArgumentException("v_run_date 轉換失敗，值為: " + RunDateRaw, "runDateRaw");
            }

            VYyyymm = yyyymm;

            // v_6m_from = v_yyyymm - 6 months
            // v_6m_to   = v_yyyymm - 5 months
            V6mFrom = VYyyymm.AddMonths(-6);
            V6mTo = VYyyymm.AddMonths(-5);

            // from_yyyymm = v_6m_from[1,4] + v_6m_from[6,7] + "01"
            // to_yyyymm   = v_6m_to[1,4] + v_6m_to[6,7] + "01"
            string fromYyyymmdd = V6mFrom.ToString("yyyyMM") + "01";
            string toYyyymmdd = V6mTo.ToString("yyyyMM") + "01";

            int fromInt;
            int toInt;

            if (!int.TryParse(fromYyyymmdd, out fromInt))
                throw new Exception("無法轉換 from_yyyymm 為整數: " + fromYyyymmdd);
            if (!int.TryParse(toYyyymmdd, out toInt))
                throw new Exception("無法轉換 to_yyyymm 為整數: " + toYyyymmdd);

            FromDateInt = fromInt;
            ToDateInt = toInt;
        }
    }
}
