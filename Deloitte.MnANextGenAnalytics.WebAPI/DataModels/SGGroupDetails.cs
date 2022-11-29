namespace Deloitte.MnANextGenAnalytics.WebAPI.DataModels
{
    public class SGGroupDetails
    {
        public int Id { get; set; }
        public string GroupName { get; set; }
        public string GroupId { get; set; }
        public bool Is_Inuse { get; set; }
        public int? Engagement_Id { get; set; }
        public string CreatedUser { get; set; }
        public DateTime CreatedDate { get; set; }
        public string ModifiedUser { get; set; }
        public DateTime ModifiedDate { get; set; }
        public bool IsActive { get; set; }


    }
}
