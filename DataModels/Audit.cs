namespace Deloitte.MnANextGenAnalytics.WebAPI.DataModels
{
    public class Audit
    {
        public int id { get; set; }
        public int engagement_Id { get; set; }
        public string modifiedUser { get; set; }
        public DateTime modifiedDate { get; set; }        
        public string createdUser { get; set; }
        public DateTime createdDate { get; set; }
        public string auditType { get; set; }
        public string oldValue { get; set; }   
        public string newValue { get; set; }   
        public bool isActive { get; set; }
    }
}
