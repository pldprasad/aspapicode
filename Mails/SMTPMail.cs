using Deloitte.MnANextGenAnalytics.WebAPI.DataModels;
using System.Net;
using System.Net.Mail;

namespace Deloitte.MnANextGenAnalytics.WebAPI.Mails
{
    public class SMTPMail
    {
        #region Private Variables
        private string _from { get; set; }
        #endregion


        static IConfigurationRoot config = new ConfigurationBuilder()
                           .SetBasePath(Path.Combine(AppContext.BaseDirectory))
                           .AddJsonFile($"appsettings.json", true)
                           .Build();
        private static ILogger _logger;
        DataLayer.DataLayer dataLayer = new DataLayer.DataLayer(config,_logger);
        public IList<string> To { get; set; }
        public string From
        {
            get { return string.IsNullOrEmpty(_from) ? config.GetValue<string>("MnANoReplyEmailId") : _from; }
            set { _from = value; }
        }
        public string Subject { get; set; }

        public string Body { get; set; }
        public string ConnectionSettings { get; set; }
        public bool IsHtml { get; set; }
        public IList<string> CC { get; set; }


        private readonly static string IsTestMail = config.GetValue<string>("SendMailToTestUser");
        private readonly static string Environment = config.GetValue<string>("Environment");
        private readonly static string Connection = config.GetValue<string>("SMTPEmailConnection");

        public void Send(EngagementDetails engagementDetails)
        {
            try
            {
                int flowType = Convert.ToInt32(engagementDetails.currentStatus);
                ConnectionSettings = Connection;
                // Covert the email connection string to dictionary
                var emailSettings = ConnectionSettings.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries).Select(part => part.Split('=')).ToDictionary(split => split[0], split => split[1]);

                bool useDefaultCredentials = Convert.ToBoolean(emailSettings.Where(e => string.Compare(e.Key, "UseDefaultCredentials", StringComparison.OrdinalIgnoreCase) == 0).Select(e => e.Value).FirstOrDefault());

                SmtpClient smtp = new SmtpClient
                {
                    Host = emailSettings.Where(e => string.Compare(e.Key, "Host", StringComparison.OrdinalIgnoreCase) == 0).Select(e => e.Value).FirstOrDefault(),
                    Port = Convert.ToInt32(emailSettings.Where(e => string.Compare(e.Key, "Port", StringComparison.OrdinalIgnoreCase) == 0).Select(e => e.Value).FirstOrDefault()),
                    EnableSsl = Convert.ToBoolean(emailSettings.Where(e => string.Compare(e.Key, "EnableSsl", StringComparison.OrdinalIgnoreCase) == 0).Select(e => e.Value).FirstOrDefault()),
                    DeliveryMethod = SmtpDeliveryMethod.Network,
                    UseDefaultCredentials = useDefaultCredentials,
                    Timeout = 100000
                };

                if (!useDefaultCredentials)
                {
                    string userName = emailSettings.Where(e => string.Compare(e.Key, "Username", StringComparison.OrdinalIgnoreCase) == 0).Select(e => e.Value).FirstOrDefault();
                    string password = emailSettings.Where(e => string.Compare(e.Key, "Password", StringComparison.OrdinalIgnoreCase) == 0).Select(e => e.Value).FirstOrDefault();
                    smtp.Credentials = new NetworkCredential(userName, password);
                }

                var message = new MailMessage
                {
                    From = new MailAddress(From),
                    IsBodyHtml = IsHtml,
                };
                if (engagementDetails.ppmdapprovers.Count > 0 && engagementDetails.ppmdapprovers != null && engagementDetails.engagementteams.Count > 0 && engagementDetails.engagementteams != null)
                {
                    //adding this flag to test real time emails
                    if (IsTestMail == "true" && Environment != "prod")
                    {
                        if (flowType == 2 || flowType == 4) {
                            foreach (Approver to in engagementDetails.ppmdapprovers)
                            {
                                message.To.Add(to.id);
                            }
                            if (flowType == 4)
                            {
                                foreach (Approver to in engagementDetails.engagementteams)
                                {
                                    message.CC.Add(to.id);
                                }
                            }
                            else
                            {
                                message.CC.Add(engagementDetails.submittedUserId);
                            }
                        }else if (flowType == 3 || flowType == 5 || flowType ==  6 || flowType == 7)
                        {
                            if (flowType == 6)
                            {
                                message.To.Add(engagementDetails.submittedUserId);
                                foreach (Approver to in engagementDetails.ppmdapprovers)
                                {
                                    message.CC.Add(to.id);
                                }
                            }
                            else
                            {
                                foreach (Approver to in engagementDetails.ppmdapprovers)
                                {
                                    message.CC.Add(to.id);
                                }
                                foreach (Approver to in engagementDetails.engagementteams)
                                {
                                    message.To.Add(to.id);
                                }
                            }
                        }
                       
                    }
                }

                //Assigning mail subject and body based on the current status type
                //PPMD Initial Submit
                if (flowType == 2)
                {
                    message.Subject = "NextGen Toolset – " + engagementDetails.engagementname + " - Request for PPMD Approval – Initial Submit";
                    message.Body = config.GetValue<string>("InitialSubmitApproval");
                }
                //Client Ready
                else if (flowType == 3)
                {
                    message.Subject = "NextGen Toolset – " + engagementDetails.engagementname + " - PPMD Approval Completed - Initial Submit";
                    message.Body = config.GetValue<string>("InitialSubmitApprovalDone");
                }
                // PPMD Client Ready
                else if (flowType == 4)
                {
                    message.Subject = "NextGen Toolset – " + engagementDetails.engagementname + " - Request for PPMD Approval – Client Ready";
                    message.Body = config.GetValue<string>("ClientReadyApproval");
                }
                //Live
                else if (flowType == 5)
                {
                    message.Subject = "NextGen Toolset – " + engagementDetails.engagementname + " - Engagement is Live";
                    message.Body = config.GetValue<string>("EngagementLive");
                }
                // Return for rework initial submit
                else if (flowType == 6)
                {
                    var Comment = dataLayer.getReturnForReworkComment(engagementDetails.id);
                    message.Subject = "NextGen Toolset – " + engagementDetails.engagementname + " - Returned for Rework";
                    message.Body = string.Format(config.GetValue<string>("ReturnForReworkEmailBody"),Comment);

                }
                // Return for rework client ready
                else if (flowType == 7)
                {
                    var Comment = dataLayer.getReturnForReworkComment(engagementDetails.id);
                    message.Subject = "NextGen Toolset – " + engagementDetails.engagementname + " - Returned for Rework";
                    message.Body = string.Format(config.GetValue<string>("ReturnForReworkEmailBody"), Comment);
                }

                message.IsBodyHtml = true;

                message.BodyEncoding = System.Text.Encoding.UTF8;
                message.SubjectEncoding = System.Text.Encoding.UTF8;

                smtp.Send(message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message);
                throw;
                //Add logging here for non sent mails
            }
        }

        /// <summary>
        /// Resets the properties
        /// </summary>
        public void Reset()
        {
            To = new List<string>();
            From = string.Empty;
            Subject = string.Empty;
            Body = string.Empty;
            CC = new List<string>();
        }
    }
}
