using static Deloitte.MnANextGenAnalytics.WebAPI.Controllers.MnAController;

namespace Deloitte.MnANextGenAnalytics.WebAPI.DataModels
{
    public class ProfileResponse
    {
        public string OdataContext { get; set; }
        public int OdataCount { get; set; }
        public List<Value> value { get; set; }
    }
    public class Value
    {
        public List<string> businessPhones { get; set; }
        public string displayName { get; set; }
        public string givenName { get; set; }
        public string jobTitle { get; set; }
        public string mail { get; set; }
        public string mobilePhone { get; set; }
        public string officeLocation { get; set; }
        public object preferredLanguage { get; set; }
        public string surname { get; set; }
        public string userPrincipalName { get; set; }
        public string id { get; set; }
        public string profilepicture { get; set; }
    }
}
