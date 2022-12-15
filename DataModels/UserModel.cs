namespace Deloitte.MnANextGenAnalytics.WebAPI.DataModels
{
    public class UserModel
    {
        public int UserId { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }

        public bool canWrite { get; set; }
    }
    
}
