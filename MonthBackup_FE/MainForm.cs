using System;
using System.Drawing;
using System.Windows.Forms;
using System.Collections.Generic;
using MonthBackup_FE.AR.Service;
using MonthBackup_FE.Helper;
using MonthBackup_FE.AR_DEL.Service;
using static MonthBackup_FE.ParameterDialog;
using MonthBackup_FE.EAR.Service;
using MonthBackup_FE.EAR_DEL.Service;
using MonthBackup_FE.AR_EXTEND.Service;
using System.CodeDom.Compiler;
using MonthBackup_FE.PRODASSYS_SPC_AR.Service;
using MonthBackup_FE.PRODASSYS_SPC_AR_DEL.Service;

namespace MonthBackup_FE
{
    public static class GlobalSettings
    {
        // 刪除模式: true = 刪除SQL資料, false = 匯出刪除資料
        public static bool IsDeleteMode { get; set; } = false;
    }
    public partial class MainForm : Form
    {
        // 儲存每個 App 的參數設定
        private Dictionary<int, Dictionary<string, string>> appParameters = new Dictionary<int, Dictionary<string, string>>();
        // 儲存每個 App 的參數顯示 Label
        private Dictionary<int, Label> paramDisplayLabels = new Dictionary<int, Label>();

        // 全域時間設定控制項
        private TextBox globalStartDateTextBox;
        private TextBox globalEndDateTextBox;
        private ComboBox deleteModeComboBox;
        private Label deleteModeDisplayLabel;
        public MainForm()
        {
            InitializeComponent();
            LogHelper.WriteLog("ProgramBase", "UI元件開始初始化");

            //string subject = "【測試】APG spc_system_backup mail 測試";
            //string body = "這是一封測試信，用來確認 SMTP 設定是否正確。";
            //string to = "MESSUP07@chipmos.com"; // 先改成你自己的信箱

            //MailHelper.SendMail(subject, body, to);


        }

        private void InitializeComponent()
        {
            this.Text = "月備份作業管理系統";
            this.Size = new Size(1000, 780);
            this.StartPosition = FormStartPosition.CenterScreen;
            this.Icon = Properties.Resources.data_backup_16;
            // 主標題
            Label titleLabel = new Label
            {
                Text = "月備份作業管理系統",
                Font = new Font("微軟正黑體", 16, FontStyle.Bold),
                Location = new Point(20, 20),
                Size = new Size(400, 30)
            };
            this.Controls.Add(titleLabel);

            // === 全域時間設定區塊 ===
            GroupBox globalTimeGroup = new GroupBox
            {
                Text = "全域時間設定",
                Font = new Font("微軟正黑體", 9, FontStyle.Bold),
                Location = new Point(480, 10),
                Size = new Size(480, 100),
                ForeColor = Color.FromArgb(0, 102, 204)
            };

            // 開始時間
            Label startDateLabel = new Label
            {
                Text = "開始時間:",
                Font = new Font("微軟正黑體", 9),
                Location = new Point(15, 25),
                Size = new Size(70, 20)
            };
            globalTimeGroup.Controls.Add(startDateLabel);

            globalStartDateTextBox = new TextBox
            {
                Location = new Point(85, 23),
                Size = new Size(120, 25),
                Font = new Font("微軟正黑體", 9),
                ForeColor = Color.Gray,
                Text = "YYYYMMDD"
            };
            globalStartDateTextBox.GotFocus += (s, e) =>
            {
                if (globalStartDateTextBox.Text == "YYYYMMDD")
                {
                    globalStartDateTextBox.Text = "";
                    globalStartDateTextBox.ForeColor = Color.Black;
                }
            };
            globalStartDateTextBox.LostFocus += (s, e) =>
            {
                if (string.IsNullOrWhiteSpace(globalStartDateTextBox.Text))
                {
                    globalStartDateTextBox.Text = "YYYYMMDD";
                    globalStartDateTextBox.ForeColor = Color.Gray;
                }
            };
            globalTimeGroup.Controls.Add(globalStartDateTextBox);

            // 結束時間
            Label endDateLabel = new Label
            {
                Text = "結束時間:",
                Font = new Font("微軟正黑體", 9),
                Location = new Point(220, 25),
                Size = new Size(70, 20)
            };
            globalTimeGroup.Controls.Add(endDateLabel);

            globalEndDateTextBox = new TextBox
            {
                Location = new Point(290, 23),
                Size = new Size(120, 25),
                Font = new Font("微軟正黑體", 9),
                ForeColor = Color.Gray,
                Text = "YYYYMMDD"
            };
            globalEndDateTextBox.GotFocus += (s, e) =>
            {
                if (globalEndDateTextBox.Text == "YYYYMMDD")
                {
                    globalEndDateTextBox.Text = "";
                    globalEndDateTextBox.ForeColor = Color.Black;
                }
            };
            globalEndDateTextBox.LostFocus += (s, e) =>
            {
                if (string.IsNullOrWhiteSpace(globalEndDateTextBox.Text))
                {
                    globalEndDateTextBox.Text = "YYYYMMDD";
                    globalEndDateTextBox.ForeColor = Color.Gray;
                }
            };
            globalTimeGroup.Controls.Add(globalEndDateTextBox);

            // 套用按鈕
            Button applyGlobalButton = new Button
            {
                Text = "套用至全部",
                Location = new Point(85, 60),
                Size = new Size(120, 30),
                BackColor = Color.FromArgb(255, 193, 7),
                ForeColor = Color.Black,
                FlatStyle = FlatStyle.Flat,
                Font = new Font("微軟正黑體", 9, FontStyle.Bold)
            };
            applyGlobalButton.Click += ApplyGlobalButton_Click;
            globalTimeGroup.Controls.Add(applyGlobalButton);

            // 清除按鈕
            Button clearGlobalButton = new Button
            {
                Text = "清除設定",
                Location = new Point(220, 60),
                Size = new Size(100, 30),
                BackColor = Color.FromArgb(220, 53, 69),
                ForeColor = Color.White,
                FlatStyle = FlatStyle.Flat,
                Font = new Font("微軟正黑體", 9)
            };
            clearGlobalButton.Click += ClearGlobalButton_Click;
            globalTimeGroup.Controls.Add(clearGlobalButton);

            this.Controls.Add(globalTimeGroup);

            GroupBox deleteModeGroup = new GroupBox
            {
                Text = "刪除模式設定",
                Font = new Font("微軟正黑體", 9, FontStyle.Bold),
                Location = new Point(20, 60),
                Size = new Size(400, 50),
                ForeColor = Color.FromArgb(220, 53, 69)
            };

            Label deleteModeLabel = new Label
            {
                Text = "刪除模式:",
                Font = new Font("微軟正黑體", 9),
                Location = new Point(15, 22),
                Size = new Size(70, 20)
            };
            deleteModeGroup.Controls.Add(deleteModeLabel);

            deleteModeComboBox = new ComboBox
            {
                Location = new Point(85, 20),
                Size = new Size(150, 25),
                Font = new Font("微軟正黑體", 9),
                DropDownStyle = ComboBoxStyle.DropDownList
            };
            deleteModeComboBox.Items.Add("匯出刪除資料");
            deleteModeComboBox.Items.Add("刪除SQL資料");
            deleteModeComboBox.SelectedIndex = 0; // 預設選擇「匯出刪除資料」
            deleteModeComboBox.SelectedIndexChanged += DeleteModeComboBox_SelectedIndexChanged;
            deleteModeGroup.Controls.Add(deleteModeComboBox);

            deleteModeDisplayLabel = new Label
            {
                Text = "目前: 匯出模式 (false)",
                Font = new Font("微軟正黑體", 8),
                Location = new Point(245, 22),
                Size = new Size(150, 20),
                ForeColor = Color.DarkGreen
            };
            deleteModeGroup.Controls.Add(deleteModeDisplayLabel);

            this.Controls.Add(deleteModeGroup);


            // 自動化執行按鈕
            Button autoRunButton = new Button
            {
                Text = "自動化執行 (1→2→3→5→6→7→4→8)",
                Font = new Font("微軟正黑體", 10, FontStyle.Bold),
                Location = new Point(480, 120),  // ← 修改這裡
                Size = new Size(400, 40),        // ← 高度調整
                BackColor = Color.FromArgb(0, 120, 215),
                ForeColor = Color.White,
                FlatStyle = FlatStyle.Flat
            };
            autoRunButton.Click += AutoRunButton_Click;
            this.Controls.Add(autoRunButton);

            // === 區段一：Unload 作業 ===
            GroupBox unloadGroup = new GroupBox
            {
                Text = "一、執行月備份相關Table資料Unload作業",
                Font = new Font("微軟正黑體", 10, FontStyle.Bold),
                Location = new Point(20, 170),
                Size = new Size(940, 190)
            };

            CreateAppPanel(unloadGroup, 1, "ar.4ge", 20, 30);
            CreateAppPanel(unloadGroup, 2, "ear.4ge", 250, 30);
            CreateAppPanel(unloadGroup, 3, "prodassys_spc_ar.4ge", 480, 30);
            CreateAppPanel(unloadGroup, 4, "ar_extend.4ge", 710, 30);

            this.Controls.Add(unloadGroup);

            // === 區段二：Delete 作業 ===
            GroupBox deleteGroup = new GroupBox
            {
                Text = "二、執行月備份相關Table資料清除作業",
                Font = new Font("微軟正黑體", 10, FontStyle.Bold),
                Location = new Point(20, 360),
                Size = new Size(940, 190)
            };

            CreateAppPanel(deleteGroup, 5, "ar_del.4ge", 20, 30);
            CreateAppPanel(deleteGroup, 6, "ear_del.4ge", 250, 30);
            CreateAppPanel(deleteGroup, 7, "prodassys_spc_ar_del.4ge", 480, 30);
            CreateAppPanel(deleteGroup, 8, "ar_extend.4ge", 710, 30);

            this.Controls.Add(deleteGroup);

            // === 區段三：ciMes ===
            GroupBox otherGroup = new GroupBox
            {
                Text = "三、ciMes(由於未能驗證正確性，暫不開放使用，若要開啟請由程式進行修改)",
                Font = new Font("微軟正黑體", 10, FontStyle.Bold),
                Location = new Point(20, 550),
                Size = new Size(940, 190)
            };

            CreateAppPanel(otherGroup, 9, "ciMes ENT", 20, 30);
            CreateAppPanel(otherGroup, 10, "ciMes LOT", 250, 30);

            this.Controls.Add(otherGroup);
        }

        // 下拉選單變更事件
        private void DeleteModeComboBox_SelectedIndexChanged(object sender, EventArgs e)
        {
            bool isDeleteMode = deleteModeComboBox.SelectedIndex == 1;

            // 如果選擇「刪除SQL資料」,跳出確認視窗
            if (isDeleteMode)
            {
                var result = MessageBox.Show(
                    "警告！\n\n" +
                    "您即將切換為「刪除SQL資料」模式\n" +
                    "此模式會直接刪除資料庫中的資料，無法復原！\n\n" +
                    "確定要切換嗎？",
                    "刪除模式確認",
                    MessageBoxButtons.YesNo,
                    MessageBoxIcon.Warning);

                if (result == DialogResult.No)
                {
                    // 使用者取消,切回「匯出刪除資料」
                    deleteModeComboBox.SelectedIndex = 0;
                    return;
                }
            }

            // 更新全域變數
            GlobalSettings.IsDeleteMode = isDeleteMode;

            string displayText = isDeleteMode ? "目前: 刪除模式 (true)" : "目前: 匯出模式 (false)";
            deleteModeDisplayLabel.Text = displayText;
            deleteModeDisplayLabel.ForeColor = isDeleteMode ? Color.DarkRed : Color.DarkGreen;

            // 記錄到 Log
            LogHelper.WriteLog("GlobalSettings", $"刪除模式已變更為: {(isDeleteMode ? "刪除SQL資料" : "匯出刪除資料")}");
        }

        // 取得刪除模式


        private void ApplyGlobalButton_Click(object sender, EventArgs e)
        {
            string startDate = globalStartDateTextBox.Text.Trim();
            string endDate = globalEndDateTextBox.Text.Trim();

            // 過濾掉 placeholder 文字
            if (startDate == "YYYYMMDD") startDate = "";
            if (endDate == "YYYYMMDD") endDate = "";

            if (string.IsNullOrEmpty(endDate))
            {
                MessageBox.Show("請至少輸入結束時間", "提醒", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return;
            }

            if (!IsValidDateFormat(endDate))
            {
                MessageBox.Show("結束時間格式錯誤，請輸入 YYYYMMDD 格式", "錯誤", MessageBoxButtons.OK, MessageBoxIcon.Error);
                return;
            }

            if (!string.IsNullOrEmpty(startDate) && !IsValidDateFormat(startDate))
            {
                MessageBox.Show("開始時間格式錯誤，請輸入 YYYYMMDD 格式", "錯誤", MessageBoxButtons.OK, MessageBoxIcon.Error);
                return;
            }

            // 套用到各個 App
            for (int i = 1; i <= 10; i++)
            {
                if (!appParameters.ContainsKey(i))
                {
                    appParameters[i] = new Dictionary<string, string>();
                }

                switch (i)
                {
                    case 1: // ar.4ge
                    case 5: // ar_del.4ge
                        appParameters[i]["endDate"] = endDate;
                        break;

                    case 2: // ear.4ge
                    case 6: // ear_del.4ge
                        if (!string.IsNullOrEmpty(startDate))
                            appParameters[i]["beg_date"] = startDate;
                        appParameters[i]["end_date"] = endDate;
                        break;

                    case 3: // prodassys_spc_ar.4ge
                    case 7: // prodassys_spc_ar_del.4ge
                        appParameters[i]["v_run_date"] = endDate;
                        break;

                    case 4: // ar_extend.4ge (UNLOAD)
                    case 8: // ar_extend.4ge (DELETE)
                        appParameters[i]["end_date"] = endDate;
                        if (!appParameters[i].ContainsKey("flag"))
                            appParameters[i]["flag"] = i == 8 ? "DELETE" : "UNLOAD";
                        break;

                    case 9: // ciMes ENT
                        if (!string.IsNullOrEmpty(startDate))
                            appParameters[i]["v_begin_date"] = startDate;
                        appParameters[i]["v_end_date"] = endDate;
                        break;
                    case 10: // ciMes LOT
                        if (!string.IsNullOrEmpty(startDate))
                            appParameters[i]["v_begin_date"] = startDate;
                        appParameters[i]["v_end_date"] = endDate;
                        break;
                }

                UpdateParameterDisplay(i);
            }

            MessageBox.Show($"已套用全域時間設定至所有項目\n開始時間: {(string.IsNullOrEmpty(startDate) ? "(未設定)" : startDate)}\n結束時間: {endDate}",
                "設定完成", MessageBoxButtons.OK, MessageBoxIcon.Information);


            string wd = $"已套用全域時間設定至所有項目  開始時間: {(string.IsNullOrEmpty(startDate) ? "(未設定)" : startDate)}n結束時間: {endDate}";
            LogHelper.WriteLog("GlobalSettings", $"已套用全域時間設定至所有項目  開始時間: {(string.IsNullOrEmpty(startDate) ? "(未設定)" : startDate)}n結束時間: {endDate}");
        }

        private void ClearGlobalButton_Click(object sender, EventArgs e)
        {
            globalStartDateTextBox.Text = "YYYYMMDD";
            globalStartDateTextBox.ForeColor = Color.Gray;
            globalEndDateTextBox.Text = "YYYYMMDD";
            globalEndDateTextBox.ForeColor = Color.Gray;
        }

        private bool IsValidDateFormat(string date)
        {
            if (date.Length != 8) return false;
            return DateTime.TryParseExact(date, "yyyyMMdd",
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.None, out _);
        }

        private void CreateAppPanel(GroupBox parent, int id, string name, int x, int y)
        {
            Panel panel = new Panel
            {
                Location = new Point(x, y),
                Size = new Size(210, 150),
                BorderStyle = BorderStyle.FixedSingle,
                BackColor = Color.White
            };

            Label nameLabel = new Label
            {
                Text = $"{id}. {name}",
                Font = new Font("微軟正黑體", 9, FontStyle.Bold),
                Location = new Point(5, 5),
                Size = new Size(200, 25),
                ForeColor = Color.FromArgb(0, 102, 204)
            };
            panel.Controls.Add(nameLabel);

            // 先透過 LogHelper 取出上次執行參數
            string lastParam = LogHelper.ExecuteRecordParser(name);
            string lastParamText = lastParam == null
                ? "上次執行參數: 無紀錄"
                : $"上次執行參數: \n{lastParam}";

            // 顯示「上次執行參數」的 Label（在參數 Label 上方）
            Label lastParamLabel = new Label
            {
                Text = lastParamText,
                Font = new Font("微軟正黑體", 7),
                Location = new Point(5, 30),
                Size = new Size(200, 30),
                ForeColor = Color.Gray,
                AutoEllipsis = true
            };
            panel.Controls.Add(lastParamLabel);

            // 原本的參數顯示 Label，往下移一點避免重疊
            Label paramDisplayLabel = new Label
            {
                Text = "參數: 尚未設定",
                Font = new Font("微軟正黑體", 7),
                Location = new Point(5, 60),
                Size = new Size(200, 45),
                ForeColor = Color.Gray,
                AutoEllipsis = true
            };
            panel.Controls.Add(paramDisplayLabel);
            paramDisplayLabels[id] = paramDisplayLabel;

            if (!name.Contains("ciMes"))
            {
                Button runButton = new Button
                {
                    Text = "執行",
                    Location = new Point(10, 110),
                    Size = new Size(85, 28),
                    BackColor = Color.FromArgb(40, 167, 69),
                    ForeColor = Color.White,
                    FlatStyle = FlatStyle.Flat,
                    Font = new Font("微軟正黑體", 9)
                };
                runButton.Click += (s, e) => RunSingleApp(id, name);
                panel.Controls.Add(runButton);
            }

            Button settingButton = new Button
            {
                Text = "設定",
                Location = new Point(105, 110),
                Size = new Size(85, 28),
                BackColor = Color.FromArgb(108, 117, 125),
                ForeColor = Color.White,
                FlatStyle = FlatStyle.Flat,
                Font = new Font("微軟正黑體", 9)
            };
            settingButton.Click += (s, e) => ShowParamDialog(id, name);
            panel.Controls.Add(settingButton);

            parent.Controls.Add(panel);
        }
        //private void CreateAppPanel(GroupBox parent, int id, string name, int x, int y)
        //{
        //    Panel panel = new Panel
        //    {
        //        Location = new Point(x, y),
        //        Size = new Size(210, 120),
        //        BorderStyle = BorderStyle.FixedSingle,
        //        BackColor = Color.White
        //    };

        //    Label nameLabel = new Label
        //    {
        //        Text = $"{id}. {name}",
        //        Font = new Font("微軟正黑體", 9, FontStyle.Bold),
        //        Location = new Point(5, 5),
        //        Size = new Size(200, 20),
        //        ForeColor = Color.FromArgb(0, 102, 204)
        //    };
        //    panel.Controls.Add(nameLabel);

        //    Label paramDisplayLabel = new Label
        //    {
        //        Text = "參數: 尚未設定",
        //        Font = new Font("微軟正黑體", 7),
        //        Location = new Point(5, 28),
        //        Size = new Size(200, 50),
        //        ForeColor = Color.Gray,
        //        AutoEllipsis = true
        //    };
        //    panel.Controls.Add(paramDisplayLabel);
        //    paramDisplayLabels[id] = paramDisplayLabel;

        //    if (!name.Contains("ciMes"))
        //    {
        //        Button runButton = new Button
        //        {
        //            Text = "執行",
        //            Location = new Point(10, 85),
        //            Size = new Size(85, 28),
        //            BackColor = Color.FromArgb(40, 167, 69),
        //            ForeColor = Color.White,
        //            FlatStyle = FlatStyle.Flat,
        //            Font = new Font("微軟正黑體", 9)
        //        };
        //        runButton.Click += (s, e) => RunSingleApp(id, name);
        //        panel.Controls.Add(runButton);
        //    }
        //    Button settingButton = new Button
        //    {
        //        Text = "設定",
        //        Location = new Point(105, 85),
        //        Size = new Size(85, 28),
        //        BackColor = Color.FromArgb(108, 117, 125),
        //        ForeColor = Color.White,
        //        FlatStyle = FlatStyle.Flat,
        //        Font = new Font("微軟正黑體", 9)
        //    };
        //    settingButton.Click += (s, e) => ShowParamDialog(id, name);
        //    panel.Controls.Add(settingButton);

        //    parent.Controls.Add(panel);
        //}

        private bool isAutoRunning = false;
        //設定需執行的程序代碼
        private List<int> autoRunSequence = new List<int> { 1,2,3,4,5,6,7,8 };
        private int currentAutoRunIndex = 0;

        // 修改 AutoRunButton_Click 方法
        private void AutoRunButton_Click(object sender, EventArgs e)
        {
            // 設定需要檢查的程序代碼
            List<int> implementedApps = new List<int> { 1, 2,3,4,5,6,7,8 };
            List<string> missingParams = new List<string>();

            foreach (int appId in implementedApps)
            {
                if (!appParameters.ContainsKey(appId) || appParameters[appId].Count == 0)
                {
                    string appName = GetAppName(appId);
                    missingParams.Add($"{appId}. {appName}");
                }
            }

            if (missingParams.Count > 0)
            {
                string message = "以下項目尚未設定參數，請先完成設定：\n\n" + string.Join("\n", missingParams);
                MessageBox.Show(message, "參數未設定", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return;
            }

            var result = MessageBox.Show(
                "確定要開始自動化執行流程嗎？\n目前執行順序：1→2→3→4→5→6→7→8(ciMes由於未能驗證正確性，暫不開放)",
                "自動化執行確認",
                MessageBoxButtons.YesNo,
                MessageBoxIcon.Question);

            if (result == DialogResult.Yes)
            {
                isAutoRunning = true;
                currentAutoRunIndex = 0;
                StartNextAutoRun();
            }
        }
        private void StartNextAutoRun()
        {

            if (currentAutoRunIndex >= autoRunSequence.Count)
            {
                isAutoRunning = false;
                MessageBox.Show("自動化執行流程已全部完成！", "完成", MessageBoxButtons.OK, MessageBoxIcon.Information);
                LogHelper.WriteLog("AutoRun", "自動化執行流程全部完成");
                return;
            }

            int appId = autoRunSequence[currentAutoRunIndex];
            string appName = GetAppName(appId);

            LogHelper.WriteLog("AutoRun", $"開始執行第 {currentAutoRunIndex + 1}/{autoRunSequence.Count} 項: {appId}. {appName}");

            Dictionary<string, string> parameters = appParameters.ContainsKey(appId)
                ? appParameters[appId]
                : new Dictionary<string, string>();

            // 創建並顯示執行視窗（自動化模式）
            ExecutionWindow execWindow = new ExecutionWindow(appId, appName, parameters, true, OnAutoExecutionCompleted);
            execWindow.Show();
        }

        // 新增：自動化執行完成回調
        private void OnAutoExecutionCompleted(int appId, bool success, string errorMessage)
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new Action<int, bool, string>(OnAutoExecutionCompleted), appId, success, errorMessage);
                return;
            }
            try
            {
                // 記錄進入回調
                LogHelper.WriteLog("AutoRun", $"OnAutoExecutionCompleted 被呼叫 - AppId: {appId}, Success: {success}, IsAutoRunning: {isAutoRunning}");

                if (!isAutoRunning)
                {
                    LogHelper.WriteLog("AutoRun", "不是自動化模式，忽略回調");
                    return;
                }

                if (success)
                {
                    LogHelper.WriteLog("AutoRun", $"項目 {appId} 執行成功");

                    // 繼續下一個
                    currentAutoRunIndex++;

                    // 使用 Application.DoEvents 確保當前操作完成
                    System.Windows.Forms.Application.DoEvents();

                    LogHelper.WriteLog("AutoRun", $"準備啟動下一個項目，索引: {currentAutoRunIndex}");

                    // 直接呼叫 StartNextAutoRun，因為我們已經在 UI 執行緒上
                    StartNextAutoRun();
                }
                else
                {
                    LogHelper.WriteLog("AutoRun", $"項目 {appId} 執行失敗: {errorMessage}");
                    isAutoRunning = false;

                    var continueResult = MessageBox.Show(
                        $"項目 {appId}. {GetAppName(appId)} 執行失敗！\n\n錯誤訊息: {errorMessage}\n\n是否繼續執行後續項目？",
                        "執行失敗",
                        MessageBoxButtons.YesNo,
                        MessageBoxIcon.Error);

                    if (continueResult == DialogResult.Yes)
                    {
                        isAutoRunning = true;
                        currentAutoRunIndex++;
                        StartNextAutoRun();
                    }
                }
            }
            catch (Exception ex)
            {
                LogHelper.WriteLog("AutoRun", $"OnAutoExecutionCompleted 發生錯誤: {ex.Message}\n{ex.StackTrace}");
                isAutoRunning = false;
                MessageBox.Show($"自動化執行發生錯誤: {ex.Message}", "錯誤", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        // 新增：根據 ID 取得 App 名稱
        private string GetAppName(int appId)
        {
            switch (appId)
            {
                case 1: return "ar.4ge";
                case 2: return "ear.4ge";
                case 3: return "prodassys_spc_ar.4ge";
                case 4: return "ar_extend.4ge";
                case 5: return "ar_del.4ge";
                case 6: return "ear_del.4ge";
                case 7: return "prodassys_spc_ar_del.4ge";
                case 8: return "ar_extend.4ge (DELETE)";
                case 9: return "ciMes ENT";
                case 10: return "ciMes LOT";
                default: return "Unknown";
            }
        }


        private void RunSingleApp(int id, string name)
        {
            if (this.InvokeRequired)
            {
                this.Invoke(new Action(() => RunSingleApp(id, name)));
                return;
            }
            if (id != 9 && id != 10)
            {
                if (!appParameters.ContainsKey(id) || appParameters[id].Count == 0)
                {
                    MessageBox.Show("請先設定參數再執行", "提醒", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                    return;
                }
            }

            Dictionary<string, string> parameters = appParameters.ContainsKey(id) ? appParameters[id] : new Dictionary<string, string>();
            ExecutionWindow execWindow = new ExecutionWindow(id, name, parameters, false, null);
            execWindow.Show();
        }

        private void ShowParamDialog(int id, string name)
        {
            if (!appParameters.ContainsKey(id))
            {
                appParameters[id] = new Dictionary<string, string>();
            }

            ParameterDialog paramDialog = new ParameterDialog(id, name, appParameters[id]);
            if (paramDialog.ShowDialog() == DialogResult.OK)
            {
                appParameters[id] = paramDialog.GetParameters();
                UpdateParameterDisplay(id);
            }
        }

        private void UpdateParameterDisplay(int id)
        {
            if (paramDisplayLabels.ContainsKey(id) && appParameters.ContainsKey(id))
            {
                var parameters = appParameters[id];
                if (parameters.Count > 0)
                {
                    string displayText = "";
                    foreach (var param in parameters)
                    {
                        if (displayText.Length > 0)
                            displayText += "\n";
                        displayText += $"{param.Key}={param.Value}";
                    }
                    paramDisplayLabels[id].Text = displayText;
                    paramDisplayLabels[id].ForeColor = Color.DarkGreen;
                }
                else
                {
                    paramDisplayLabels[id].Text = "參數: 尚未設定";
                    paramDisplayLabels[id].ForeColor = Color.Gray;
                }
            }
        }
    }

    public class ExecutionWindow : Form
    {

        //private TextBox logTextBox;
        //private Label statusLabel;
        //private int appId;
        //private string appName;
        //private Dictionary<string, string> parameters;
        //private System.Threading.Thread executionThread;
        private TextBox logTextBox;
        private Label statusLabel;
        private int appId;
        private string appName;
        private Dictionary<string, string> parameters;
        private System.Threading.Thread executionThread;
        private bool isAutoMode;
        private bool executionSuccess = false;
        private string errorMessage = "";
        private Action<int, bool, string> autoCompletionCallback;

        // 新增事件：執行完成事件
        public event EventHandler<ExecutionResult> ExecutionCompleted;

        //public ExecutionWindow(int id, string name, Dictionary<string, string> appParameters)
        //{
        //    this.appId = id;
        //    this.appName = name;
        //    this.parameters = new Dictionary<string, string>(appParameters);

        //    this.Text = $"執行狀況 - {name}";
        //    this.Size = new Size(700, 500);
        //    this.StartPosition = FormStartPosition.CenterParent;
        //    this.FormClosing += ExecutionWindow_FormClosing;

        //    statusLabel = new Label
        //    {
        //        Text = $"正在執行: {id}. {name}",
        //        Font = new Font("微軟正黑體", 12, FontStyle.Bold),
        //        Location = new Point(20, 20),
        //        Size = new Size(650, 30),
        //        ForeColor = Color.FromArgb(0, 102, 204)
        //    };
        //    this.Controls.Add(statusLabel);

        //    logTextBox = new TextBox
        //    {
        //        Multiline = true,
        //        ScrollBars = ScrollBars.Both,
        //        Location = new Point(20, 60),
        //        Size = new Size(650, 370),
        //        Font = new Font("Consolas", 9),
        //        ReadOnly = true,
        //        BackColor = Color.Black,
        //        ForeColor = Color.Lime
        //    };
        //    this.Controls.Add(logTextBox);

        //    Button closeButton = new Button
        //    {
        //        Text = "關閉",
        //        Location = new Point(590, 440),
        //        Size = new Size(80, 30),
        //        BackColor = Color.FromArgb(220, 53, 69),
        //        ForeColor = Color.White,
        //        FlatStyle = FlatStyle.Flat
        //    };
        //    closeButton.Click += (s, e) => this.Close();
        //    this.Controls.Add(closeButton);

        //    StartExecution();
        //}
        public ExecutionWindow(int id, string name, Dictionary<string, string> appParameters)
        : this(id, name, appParameters, false, null)
                {
                }

        public ExecutionWindow(int id, string name, Dictionary<string, string> appParameters,
                   bool autoMode, Action<int, bool, string> autoCallback)
        {
            this.Icon = Properties.Resources.data_backup_16;
            this.appId = id;
            this.appName = name;
            this.parameters = new Dictionary<string, string>(appParameters);
            this.isAutoMode = autoMode;
            this.autoCompletionCallback = autoCallback;

            this.Text = $"執行狀況 - {name}" + (autoMode ? " (自動化)" : "");
            this.Size = new Size(700, 520);
            this.StartPosition = FormStartPosition.CenterParent;
            this.FormClosing += ExecutionWindow_FormClosing;

            statusLabel = new Label
            {
                Text = $"正在執行: {id}. {name}" + (autoMode ? " [自動化模式]" : ""),
                Font = new Font("微軟正黑體", 12, FontStyle.Bold),
                Location = new Point(20, 20),
                Size = new Size(650, 30),
                ForeColor = Color.FromArgb(0, 102, 204)
            };
            this.Controls.Add(statusLabel);

            logTextBox = new TextBox
            {
                Multiline = true,
                ScrollBars = ScrollBars.Both,
                Location = new Point(20, 60),
                Size = new Size(650, 370),
                Font = new Font("Consolas", 9),
                ReadOnly = true,
                BackColor = Color.Black,
                ForeColor = Color.Lime
            };
            this.Controls.Add(logTextBox);

            Button closeButton = new Button
            {
                Text = "關閉",
                Location = new Point(590, 440),
                Size = new Size(80, 30),
                BackColor = Color.FromArgb(220, 53, 69),
                ForeColor = Color.White,
                FlatStyle = FlatStyle.Flat,
                Enabled = !autoMode
            };
            closeButton.Click += (s, e) => this.Close();
            this.Controls.Add(closeButton);

            if (autoMode)
            {
                Label autoLabel = new Label
                {
                    Text = "⚡ 自動化執行中...",
                    Font = new Font("微軟正黑體", 9, FontStyle.Bold),
                    Location = new Point(20, 440),
                    Size = new Size(200, 30),
                    ForeColor = Color.Orange
                };
                this.Controls.Add(autoLabel);
            }

            //StartExecution();
            this.Load += (s, e) => {
                StartExecution();
            };
        }


        private void ExecutionWindow_FormClosing(object sender, FormClosingEventArgs e)
        {
            if (isAutoMode && executionThread != null && executionThread.IsAlive)
            {
                // 自動化模式下不允許手動關閉執行中的視窗
                e.Cancel = true;
                return;
            }

            if (!isAutoMode && executionThread != null && executionThread.IsAlive)
            {
                var result = MessageBox.Show("執行中，確定要關閉視窗嗎？", "確認", MessageBoxButtons.YesNo, MessageBoxIcon.Question);
                if (result == DialogResult.No)
                {
                    e.Cancel = true;
                }
            }
        }

        private void StartExecution()
        {
            executionThread = new System.Threading.Thread(() =>
            {
                try
                {
                    switch (appId)
                    {
                        case 1:
                            ArExportService.Execute(parameters, AppendLog);
                            break;
                        case 2:
                            EarExportExecutor.Execute(parameters, AppendLog);
                            System.Threading.Thread.Sleep(2000);
                            break;
                        case 3:
                            //AppendLog("prodassys_spc_ar.4ge 執行邏輯開發中...");
                            ProdassysSpcArExecutor.Execute(parameters, AppendLog);
                            System.Threading.Thread.Sleep(2000);
                            break;
                        case 4:
                            //AppendLog("ar_extend.4ge 執行邏輯開發中...");
                            ArExtendExecutor.Execute(parameters, AppendLog);
                            System.Threading.Thread.Sleep(2000);
                            break;
                        case 5:
                            ArDelExportService.Execute(parameters, AppendLog);
                                                        System.Threading.Thread.Sleep(2000);
                            break;
                        case 6:
                            EarDeleteExecutor.Execute(parameters, AppendLog);
                            System.Threading.Thread.Sleep(2000);

                            break;
                        case 7:
                            //AppendLog("prodassys_spc_ar_del.4ge 執行邏輯開發中...");
                            ProdassysSpcArDelExecutor.Execute(parameters, AppendLog);

                            System.Threading.Thread.Sleep(2000);
                            break;
                        case 8:
                            //AppendLog("ar_extend.4ge (DELETE) 執行邏輯開發中...");
                            ArExtendExecutor.Execute(parameters, AppendLog);
                            System.Threading.Thread.Sleep(2000);
                            break;
                        case 9:
                            // 第 9 個功能：EMS 設備歷史歸檔
                            // 參數：v_begin_date / v_end_date (YYYYMMDD)
                            MonthBackup_FE.ciMesArchive.Eqp.Service.EquipArchiveExecutor.Execute(parameters, AppendLog);
                            System.Threading.Thread.Sleep(2000);
                            break;
                        case 10:
                            //AppendLog("ciMes LOT 執行邏輯開發中...");
                            MonthBackup_FE.ciMesArchive.Lot.Service.LotArchiveExecutor.Execute(parameters, AppendLog);
                            System.Threading.Thread.Sleep(2000);
                            break;
                        default:
                            throw new Exception($"未知的 App ID: {appId}");
                    }

                    executionSuccess = true;
                    UpdateStatus("執行完成", Color.Green);
                    AppendLog("\n=== 執行成功完成 ===");

                    // 觸發完成回調
                    OnExecutionCompleted(true, "");
                }
                catch (Exception ex)
                {
                    executionSuccess = false;
                    errorMessage = ex.Message;

                    AppendLog($"\n[X] 執行錯誤: {ex.Message}");
                    AppendLog($"[X] 堆疊追蹤:\n{ex.StackTrace}");
                    UpdateStatus("執行失敗", Color.Red);

                    // 觸發完成回調
                    OnExecutionCompleted(false, ex.Message);
                }
            });

            executionThread.IsBackground = true;
            executionThread.Start();
        }

        private void OnExecutionCompleted(bool success, string error)
        {
            try
            {
                // 記錄詳細資訊
                LogHelper.WriteLog("ExecutionWindow", $"OnExecutionCompleted 被呼叫 - AppId: {appId}, Success: {success}, IsAutoMode: {isAutoMode}, HasCallback: {autoCompletionCallback != null}, ThreadId: {System.Threading.Thread.CurrentThread.ManagedThreadId}");

                // 嚴格檢查：必須同時滿足自動化模式且有回調函數
                if (!isAutoMode || autoCompletionCallback == null)
                {
                    LogHelper.WriteLog("ExecutionWindow", "單一執行模式，不處理回調");
                    return;
                }

                LogHelper.WriteLog("ExecutionWindow", "自動化模式，準備處理回調");

                // 自動化模式：使用 BeginInvoke 在 UI 執行緒上關閉視窗並呼叫回調
                if (this.IsDisposed || this.Disposing)
                {
                    LogHelper.WriteLog("ExecutionWindow", "視窗已釋放，無法處理");
                    return;
                }

                LogHelper.WriteLog("ExecutionWindow", $"InvokeRequired: {this.InvokeRequired}");

                this.BeginInvoke(new Action(() =>
                {
                    try
                    {
                        LogHelper.WriteLog("ExecutionWindow", "BeginInvoke 回調執行中");

                        // 先關閉視窗
                        if (!this.IsDisposed)
                        {
                            LogHelper.WriteLog("ExecutionWindow", "準備關閉視窗");
                            this.Close();
                            LogHelper.WriteLog("ExecutionWindow", "視窗已關閉");
                        }

                        // 強制處理訊息佇列
                        System.Windows.Forms.Application.DoEvents();

                        // 直接在這裡呼叫回調，不使用 Timer
                        LogHelper.WriteLog("ExecutionWindow", $"準備呼叫回調函數 - AppId: {appId}, ThreadId: {System.Threading.Thread.CurrentThread.ManagedThreadId}");

                        // 再次確認回調存在才呼叫
                        if (autoCompletionCallback != null)
                        {
                            autoCompletionCallback(appId, success, error);
                            LogHelper.WriteLog("ExecutionWindow", "回調函數已呼叫");
                        }
                        else
                        {
                            LogHelper.WriteLog("ExecutionWindow", "回調函數為 null，無法呼叫");
                        }
                    }
                    catch (Exception ex)
                    {
                        LogHelper.WriteLog("ExecutionWindow", $"BeginInvoke 內部發生錯誤: {ex.Message}\n{ex.StackTrace}");
                    }
                }));
            }
            catch (ObjectDisposedException ex)
            {
                LogHelper.WriteLog("ExecutionWindow", $"視窗已釋放: {ex.Message}");
            }
            catch (Exception ex)
            {
                LogHelper.WriteLog("ExecutionWindow", $"OnExecutionCompleted 發生錯誤: {ex.Message}\n{ex.StackTrace}");
            }
        }




        //private void AppendLog(string message)
        //{
        //    if (logTextBox == null || logTextBox.IsDisposed)
        //        return;

        //    try
        //    {
        //        if (logTextBox.InvokeRequired)
        //        {
        //            logTextBox.Invoke(new Action<string>(AppendLog), message);
        //        }
        //        else
        //        {
        //            logTextBox.AppendText($"[{DateTime.Now:HH:mm:ss}] {message}\r\n");
        //        }
        //    }
        //    catch (ObjectDisposedException) { }
        //    catch (Exception ex)
        //    {
        //        System.Diagnostics.Debug.WriteLine($"Log Error: {ex.Message}");
        //    }
        //}
        private void AppendLog(string message)
        {
            // 1. 檢查控制項是否存在且 Handle 是否已建立 (重要：解決第二次執行或關閉時的報錯)
            if (logTextBox == null || logTextBox.IsDisposed || !logTextBox.IsHandleCreated)
                return;

            try
            {
                if (logTextBox.InvokeRequired)
                {
                    // 使用 BeginInvoke 代替 Invoke (非同步呼叫)
                    // 理由：Invoke 是同步等待，若 UI 執行緒正忙著關閉視窗，這裡會死鎖 (Deadlock)
                    logTextBox.BeginInvoke(new Action<string>(AppendLog), message);
                }
                else
                {
                    logTextBox.AppendText($"[{DateTime.Now:HH:mm:ss}] {message}\r\n");
                    // 建議加上自動捲動，方便查看最新日誌
                    logTextBox.SelectionStart = logTextBox.Text.Length;
                    logTextBox.ScrollToCaret();
                }
            }
            catch (Exception ex)
            {
                // 忽略視窗關閉時產生的預期錯誤
                if (!(ex is ObjectDisposedException || ex is InvalidOperationException))
                {
                    System.Diagnostics.Debug.WriteLine($"Log Error: {ex.Message}");
                }
            }
        }

        //private void UpdateStatus(string status, Color color)
        //{
        //    if (statusLabel.InvokeRequired)
        //    {
        //        statusLabel.Invoke(new Action<string, Color>(UpdateStatus), status, color);
        //    }
        //    else
        //    {
        //        statusLabel.Text = $"{appId}. {appName} - {status}";
        //        statusLabel.ForeColor = color;
        //    }
        //}
        private void UpdateStatus(string status, Color color)
        {
            // 關鍵檢查：如果 label 已經被回收或尚未建立 Handle，直接回傳
            if (statusLabel == null || statusLabel.IsDisposed || !statusLabel.IsHandleCreated)
                return;

            try
            {
                if (statusLabel.InvokeRequired)
                {
                    // 改用 BeginInvoke (非同步更新)，避免背景執行緒被 UI 卡死
                    statusLabel.BeginInvoke(new Action<string, Color>(UpdateStatus), status, color);
                }
                else
                {
                    statusLabel.Text = $"{appId}. {appName} - {status}";
                    statusLabel.ForeColor = color;
                }
            }
            catch (Exception ex)
            {
                // 捕捉視窗關閉瞬間產生的存取錯誤
                System.Diagnostics.Debug.WriteLine($"UpdateStatus Error: {ex.Message}");
            }
        }
    }

    public class ParameterDialog : Form
    {
        private Dictionary<string, Control> paramControls = new Dictionary<string, Control>();
        private Dictionary<string, string> currentParameters;

        public ParameterDialog(int id, string name, Dictionary<string, string> existingParams)
        {
            this.currentParameters = new Dictionary<string, string>(existingParams);

            this.Text = $"參數設定 - {name}";
            this.Size = new Size(500, 450);
            this.StartPosition = FormStartPosition.CenterParent;
            this.FormBorderStyle = FormBorderStyle.FixedDialog;
            this.MaximizeBox = false;
            this.MinimizeBox = false;

            Label titleLabel = new Label
            {
                Text = $"設定 {id}. {name} 的執行參數",
                Font = new Font("微軟正黑體", 11, FontStyle.Bold),
                Location = new Point(20, 20),
                Size = new Size(450, 25)
            };
            this.Controls.Add(titleLabel);

            int yPos = 60;

            switch (id)
            {
                case 1:
                case 5:
                    CreateParamField("endDate", "endDate (截止日期 YYYYMMDD):", ref yPos);
                    break;
                case 2:
                case 6:
                    CreateParamField("beg_date", "beg_date (開始日期 YYYYMMDD):", ref yPos);
                    CreateParamField("end_date", "end_date (結束日期 YYYYMMDD):", ref yPos);
                    break;
                case 3:
                case 7:
                    CreateParamField("v_run_date", "v_run_date (執行日期 YYYYMMDD):", ref yPos);
                    break;
                case 4:
                case 8:
                    CreateParamField("end_date", "end_date (截止日期 YYYYMMDD):", ref yPos);
                    CreateParamComboBox("flag", "flag (操作類型):", new[] { "UNLOAD", "DELETE" }, ref yPos, id == 8 ? "DELETE" : "UNLOAD");
                    CreateParamField("file_name", "file_name (檔案名稱):", ref yPos);
                    break;
                case 9: // ciMes ENT - 第 9 個功能：MES EQP ARCHIVE
                    CreateParamField("v_begin_date", "v_begin_date (開始日期 YYYYMMDD，可空白):", ref yPos);
                    CreateParamField("v_end_date", "v_end_date (結束日期 YYYYMMDD):", ref yPos);
                    break;
                case 10:// ciMes ENT - 第 10 個功能：MES LOT ARCHIVE
                    CreateParamField("v_begin_date", "v_begin_date (開始日期 YYYYMMDD，可空白):", ref yPos);
                    CreateParamField("v_end_date", "v_end_date (結束日期 YYYYMMDD):", ref yPos);
                    break;
            }

            Button saveButton = new Button
            {
                Text = "儲存",
                Location = new Point(300, 370),
                Size = new Size(80, 35),
                BackColor = Color.FromArgb(40, 167, 69),
                ForeColor = Color.White,
                FlatStyle = FlatStyle.Flat,
                DialogResult = DialogResult.OK
            };
            saveButton.Click += SaveButton_Click;
            this.Controls.Add(saveButton);

            Button cancelButton = new Button
            {
                Text = "取消",
                Location = new Point(390, 370),
                Size = new Size(80, 35),
                BackColor = Color.FromArgb(108, 117, 125),
                ForeColor = Color.White,
                FlatStyle = FlatStyle.Flat,
                DialogResult = DialogResult.Cancel
            };
            this.Controls.Add(cancelButton);
        }

        private void CreateParamField(string paramName, string labelText, ref int yPos)
        {
            Label label = new Label
            {
                Text = labelText,
                Location = new Point(20, yPos),
                Size = new Size(450, 20),
                Font = new Font("微軟正黑體", 9)
            };
            this.Controls.Add(label);

            TextBox textBox = new TextBox
            {
                Location = new Point(20, yPos + 25),
                Size = new Size(450, 25),
                Font = new Font("微軟正黑體", 9)
            };

            if (currentParameters.ContainsKey(paramName))
            {
                textBox.Text = currentParameters[paramName];
            }

            paramControls[paramName] = textBox;
            this.Controls.Add(textBox);

            yPos += 65;
        }

        private void CreateParamComboBox(string paramName, string labelText, string[] options, ref int yPos, string defaultValue = null)
        {
            Label label = new Label
            {
                Text = labelText,
                Location = new Point(20, yPos),
                Size = new Size(450, 20),
                Font = new Font("微軟正黑體", 9)
            };
            this.Controls.Add(label);

            ComboBox comboBox = new ComboBox
            {
                Location = new Point(20, yPos + 25),
                Size = new Size(450, 25),
                Font = new Font("微軟正黑體", 9),
                DropDownStyle = ComboBoxStyle.DropDownList
            };
            comboBox.Items.AddRange(options);

            if (currentParameters.ContainsKey(paramName))
            {
                int index = Array.IndexOf(options, currentParameters[paramName]);
                if (index >= 0)
                {
                    comboBox.SelectedIndex = index;
                }
            }
            else if (!string.IsNullOrEmpty(defaultValue))
            {
                int index = Array.IndexOf(options, defaultValue);
                if (index >= 0)
                {
                    comboBox.SelectedIndex = index;
                }
            }
            else if (options.Length > 0)
            {
                comboBox.SelectedIndex = 0;
            }

            paramControls[paramName] = comboBox;
            this.Controls.Add(comboBox);

            yPos += 65;
        }

        private void SaveButton_Click(object sender, EventArgs e)
        {
            foreach (var kvp in paramControls)
            {
                string value = "";
                if (kvp.Value is TextBox)
                {
                    value = ((TextBox)kvp.Value).Text.Trim();
                }
                else if (kvp.Value is ComboBox)
                {
                    value = ((ComboBox)kvp.Value).SelectedItem?.ToString() ?? "";
                }

                if (string.IsNullOrEmpty(value))
                {
                    MessageBox.Show($"請輸入 {kvp.Key} 參數", "警告", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                    this.DialogResult = DialogResult.None;
                    return;
                }
            }
        }

        public Dictionary<string, string> GetParameters()
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();

            foreach (var kvp in paramControls)
            {
                if (kvp.Value is TextBox)
                {
                    parameters[kvp.Key] = ((TextBox)kvp.Value).Text.Trim();
                }
                else if (kvp.Value is ComboBox)
                {
                    parameters[kvp.Key] = ((ComboBox)kvp.Value).SelectedItem?.ToString() ?? "";
                }
            }

            return parameters;
        }

        public class ExecutionResult : EventArgs
        {
            public int AppId { get; set; }
            public string AppName { get; set; }
            public bool Success { get; set; }
            public string ErrorMessage { get; set; }
        }
    }
}