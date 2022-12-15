namespace Deloitte.MnANextGenAnalytics.WebAPI.DataModels
{
    public class UserModelApprover
    {
        public string displayName { get; set; }
        public string id { get; set; }
        public string imgUrl { get; set; }
        public string givenName { get; set; }
        public string surName { get; set; }
        public bool readPermission { get; set; }
        public bool writePermission { get; set; }
    }
}
