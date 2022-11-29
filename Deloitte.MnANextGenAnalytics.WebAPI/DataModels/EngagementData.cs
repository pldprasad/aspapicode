namespace Deloitte.MnANextGenAnalytics.WebAPI.DataModels
{
    public class EngagementData
    {
        public int id { get; set; }
        public DateTime createdDate{get;set;}
        public string submittedBy{get;set;}
        public string submittedUserId { get;set;}
        public string engagementName { get; set; }
        public int offeringPortfolio { get; set; }
        public string wbsCode { get; set; }
        public string adlsFolderName { get; set; }
        public string adlsFolderUrl { get; set; }
        public bool engagementisBuySide { get; set; }
        public int currentStatus { get; set; }
        public int previousStatus { get; set; }
        public bool userhasWritePermission { get; set; }
        public string tableauURL { get; set; }
        public List<string> dataSets { get; set; }
        public List<UserModelApprover> ppmdApprovers { get; set; }
        public List<UserModelApprover> engagementTeams { get; set; }
        public List<UserModel> clientTeams { get; set; }

    }
}
