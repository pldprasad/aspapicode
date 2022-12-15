using Deloitte.MnANextGenAnalytics.WebAPI.DataModels;
using System.Data;
using System.Data.Odbc;
using System.IO.Hashing;

namespace Deloitte.MnANextGenAnalytics.WebAPI.DataLayer
{
    public class DataLayer
    {
        private readonly IConfiguration _config;
        private readonly ILogger _logger;

        public DataLayer(IConfiguration config, ILogger logger)
        {
            _config = config;
            _logger = logger;
        }
        public string GetODBCConnection()
        {
            var Driver = _config.GetValue<string>("DatabricksConnection:Driver");
            var DSN = _config.GetValue<string>("DatabricksConnection:DSN");
            var Host = _config.GetValue<string>("DatabricksConnection:Host");
            var Port = _config.GetValue<string>("DatabricksConnection:Port");
            var SSL = _config.GetValue<string>("DatabricksConnection:SSL");
            var ThriftTransport = _config.GetValue<string>("DatabricksConnection:ThriftTransport");
            var AuthMech = _config.GetValue<string>("DatabricksConnection:AuthMech");
            var UID = _config.GetValue<string>("DatabricksConnection:UID");
            var PWD = _config.GetValue<string>("DatabricksConnection:PWD");
            var HTTPPath = _config.GetValue<string>("DatabricksConnection:HTTPPath");
            OdbcConnectionStringBuilder odbcConnectionStringBuilder = new OdbcConnectionStringBuilder
            {
                Driver = Driver,
                Dsn = DSN

            };
            odbcConnectionStringBuilder.Add("Host", Host);
            odbcConnectionStringBuilder.Add("Port", Port);
            odbcConnectionStringBuilder.Add("SSL", SSL);
            odbcConnectionStringBuilder.Add("ThriftTransport", ThriftTransport);
            odbcConnectionStringBuilder.Add("AuthMech", AuthMech);
            odbcConnectionStringBuilder.Add("UID", UID);
            odbcConnectionStringBuilder.Add("PWD", PWD);
            odbcConnectionStringBuilder.Add("HTTPPath", HTTPPath);

            return odbcConnectionStringBuilder.ConnectionString;
        }

        public bool ValidateUser(string EmailId)
        {
            try
            {
                string connectionString = GetODBCConnection();
                string query1;
                OdbcCommand? command = null;

                using (OdbcConnection connection = new OdbcConnection(connectionString))
                {
                    connection.Open();
                    query1 = String.Format("select count(*) from mna.User_Access where EmailId = '{0}' and IsActive=true", EmailId);
                    command = new OdbcCommand(query1, connection);

                    int count = 0;
                    count = Convert.ToInt32(command.ExecuteScalar());
                    if (count > 0)
                    {
                        return true;
                    }
                    return false;

                }
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public int UpdateEngagementDetails(EngagementDetails data)
        {
            try
            {
                string connectionString = GetODBCConnection();
                string query1 = "";
                string date = "getdate()";
                int rowAffected = 0;
                int userId = 0;
                OdbcCommand? command = null;
                OdbcDataReader? reader = null;

                using (OdbcConnection connection = new OdbcConnection(connectionString))
                {
                    connection.Open();
                    //adding the data for testing
                    //updating the engagement details details                    
                    query1 = string.Format("Update mna.engagement_details set Engagement_Name = '{0}'," +
                    "Offering_Portfolio_Id='{1}'," +
                   "WBS_Code='{2}'," +
                   "Is_Engagement_Buy_Side={3}," +
                   "Current_state={4}," +
                   "IsActive={5}," +
                   "CreatedUser='{6}'," +
                   "CreatedDate={7}," +
                   "ModifiedUser='{8}'," +
                   "ModifiedDate={9}," +
                   "Previous_state={10}" +
                   " Where Id={11}", data.engagementname, data.offeringPortfolio, data.wbscode, data.engagementisbuyside, data.currentStatus, true, data.submittedby, date, data.submittedby, date, data.previousStatus, data.id);

                    command = new OdbcCommand(query1, connection);
                    rowAffected = command.ExecuteNonQuery();
                    //deleting the all users data for the update purpose
                    query1 = String.Format("delete from mna.user_roles where Engagement_Id= {0}", data.id);
                    command = new OdbcCommand(query1, connection);
                    rowAffected = command.ExecuteNonQuery();

                    //adding the data to users table
                    if ((data.ppmdapprovers != null) && (data.ppmdapprovers.Any()))
                    {
                        foreach (var ppmdapprover in data.ppmdapprovers)
                        {
                            AddAccessUser(ppmdapprover.id, data.submittedby);
                            query1 = String.Format("select count(*) from mna.users where EmailId = '{0}'", ppmdapprover.id);
                            command = new OdbcCommand(query1, connection);

                            int count = 0;
                            count = Convert.ToInt32(command.ExecuteScalar());

                            if (count == 0)
                            {
                                query1 = String.Format("insert into mna.users(Alias," +
                                "Last_Name," +
                                "First_Name," +
                                "IsPPMD," +
                                "EmailID," +
                                "Domain," +
                                "Office," +
                                "IsActive," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "Title) values('{0}','{1}','{2}',{3},'{4}','{5}','{6}','{7}','{8}',{9},'{10}',{11},'{12}')", ppmdapprover.displayName, ppmdapprover.displayName, ppmdapprover.displayName, true, ppmdapprover.id, "Domain", "Office", true, data.submittedby, date, data.submittedby, date, "title");

                                command = new OdbcCommand(query1, connection);
                                rowAffected = command.ExecuteNonQuery();
                            }

                            //Adding user to user role table (user and engagement details)

                            query1 = string.Format("select id from mna.users where EmailID='{0}'", ppmdapprover.id);
                            command = new OdbcCommand(query1, connection);
                            reader = command.ExecuteReader();
                            userId = 0;
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    userId = reader.GetInt32(0);
                                }
                            }

                            if (userId > 0)
                            {
                                query1 = String.Format("insert into mna.user_roles(UserId," +
                                "Engagement_Id," +
                                "Role_Id," +
                                "Can_Write," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "IsActive) values({0},{1},{2},{3},'{4}',{5},'{6}',{7},{8})", userId, data.id, 1, ppmdapprover.writePermission, data.submittedby, date, data.submittedby, date, true);

                                command = new OdbcCommand(query1, connection);

                                rowAffected = command.ExecuteNonQuery();

                            }
                        }



                    }

                    //adding team member to user table
                    if ((data.engagementteams != null) && (data.engagementteams.Any()))
                    {
                        foreach (var engagementteam in data.engagementteams)
                        {
                            AddAccessUser(engagementteam.id, data.submittedby);
                            query1 = String.Format("select count(*) from mna.users where EmailId = '{0}'", engagementteam.id);
                            command = new OdbcCommand(query1, connection);

                            int count = 0;
                            count = Convert.ToInt32(command.ExecuteScalar());

                            if (count == 0)
                            {
                                query1 = String.Format("insert into mna.users(Alias," +
                                "Last_Name," +
                                "First_Name," +
                                "IsPPMD," +
                                "EmailID," +
                                "Domain," +
                                "Office," +
                                "IsActive," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "Title) values('{0}','{1}','{2}',{3},'{4}','{5}','{6}','{7}','{8}',{9},'{10}',{11},'{12}')", engagementteam.displayName, engagementteam.displayName, engagementteam.displayName, false, engagementteam.id, "Domain", "Office", true, data.submittedby, date, data.submittedby, date, "title");

                                command = new OdbcCommand(query1, connection);
                                rowAffected = command.ExecuteNonQuery();
                            }

                            //Adding user to user role table (user and engagement details)

                            query1 = string.Format("select id from mna.users where EmailID='{0}'", engagementteam.id);
                            command = new OdbcCommand(query1, connection);
                            reader = command.ExecuteReader();
                            userId = 0;
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    userId = reader.GetInt32(0);
                                }
                            }

                            if (userId > 0)
                            {
                                query1 = String.Format("insert into mna.user_roles(UserId," +
                                "Engagement_Id," +
                                "Role_Id," +
                                "Can_Write," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "IsActive) values({0},{1},{2},{3},'{4}',{5},'{6}',{7},{8})", userId, data.id, 2, engagementteam.writePermission, data.submittedby, date, data.submittedby, date, true);

                                command = new OdbcCommand(query1, connection);

                                rowAffected = command.ExecuteNonQuery();

                            }
                        }



                    }

                    // adding client menmber to the table
                    if ((data.clientTeams != null) && (data.clientTeams.Any()))
                    {
                        foreach (var clienntteam in data.clientTeams)
                        {

                            query1 = String.Format("select count(*) from mna.users where EmailId = '{0}'", clienntteam.email);
                            command = new OdbcCommand(query1, connection);

                            int count = 0;
                            count = Convert.ToInt32(command.ExecuteScalar());
                            string fullName = "" + clienntteam.firstName + " " + clienntteam.lastName;

                            if (count == 0)
                            {
                                query1 = String.Format("insert into mna.users(Alias," +
                                "Last_Name," +
                                "First_Name," +
                                "IsPPMD," +
                                "EmailID," +
                                "Domain," +
                                "Office," +
                                "IsActive," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "Title) values('{0}','{1}','{2}',{3},'{4}','{5}','{6}','{7}','{8}',{9},'{10}',{11},'{12}')", fullName, clienntteam.lastName, clienntteam.firstName, false, clienntteam.email, "Domain", "Office", true, data.submittedby, date, data.submittedby, date, "title");

                                command = new OdbcCommand(query1, connection);
                                rowAffected = command.ExecuteNonQuery();
                            }

                            //Adding user to user role table (user and engagement details)

                            query1 = string.Format("select id from mna.users where EmailID='{0}'", clienntteam.email);
                            command = new OdbcCommand(query1, connection);
                            reader = command.ExecuteReader();
                            userId = 0;
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    userId = reader.GetInt32(0);
                                }
                            }

                            if (userId > 0)
                            {
                                query1 = String.Format("insert into mna.user_roles(UserId," +
                                "Engagement_Id," +
                                "Role_Id," +
                                "Can_Write," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "IsActive) values({0},{1},{2},{3},'{4}',{5},'{6}',{7},{8})", userId, data.id, 3, false, data.submittedby, date, data.submittedby, date, true);

                                command = new OdbcCommand(query1, connection);

                                rowAffected = command.ExecuteNonQuery();

                            }
                        }



                    }




                    return (int)data.id;
                }
            }
            catch (Exception e)
            {
                _logger.LogError("UpdateEngagementDetails : " + e.Message);
                return -1;
            }

        }

        public int SaveEngagementDetails(EngagementDetails data)
        {
            string connectionString = GetODBCConnection();
            string resp = "";
            string query1 = "";
            string date = "getdate()";
            int latestEngagementId = 0;
            int userId = 0;
            int rowAffected = 0;

            //adding the engagement details to table
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                connection.Open();
                try
                {
                    query1 = string.Format("INSERT into mna.engagement_details(Engagement_Name," +
                    "Offering_Portfolio_Id," +
                    "WBS_Code," +
                    "Is_Engagement_Buy_Side," +
                    "Current_state," +
                    "IsActive," +
                    "CreatedUser," +
                    "CreatedDate," +
                    "ModifiedUser," +
                    "ModifiedDate," +
                    "Previous_state) values('{0}',{1},'{2}',{3},{4},{5},'{6}',{7},'{8}',{9},{10})", data.engagementname, data.offeringPortfolio, data.wbscode, data.engagementisbuyside, data.currentStatus, true, data.submittedby, date, data.submittedby, date, data.previousStatus);

                    OdbcCommand command = new OdbcCommand(query1, connection);
                    rowAffected = command.ExecuteNonQuery();

                    query1 = "select  Id  from mna.engagement_details  order by Id desc limit 1";
                    command = new OdbcCommand(query1, connection);
                    OdbcDataReader reader = command.ExecuteReader();

                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            latestEngagementId = reader.GetInt32(0);
                        }
                    }

                    //adding ppmd  to users table
                    if ((data.ppmdapprovers != null) && (data.ppmdapprovers.Any()))
                    {
                        foreach (var ppmdapprover in data.ppmdapprovers)
                        {

                            AddAccessUser(ppmdapprover.id, data.submittedby);
                            query1 = String.Format("select count(*) from mna.users where EmailId = '{0}'", ppmdapprover.id);
                            command = new OdbcCommand(query1, connection);

                            int count = 0;
                            count = Convert.ToInt32(command.ExecuteScalar());

                            if (count == 0)
                            {
                                query1 = String.Format("insert into mna.users(Alias," +
                                "Last_Name," +
                                "First_Name," +
                                "IsPPMD," +
                                "EmailID," +
                                "Domain," +
                                "Office," +
                                "IsActive," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "Title) values('{0}','{1}','{2}',{3},'{4}','{5}','{6}','{7}','{8}',{9},'{10}',{11},'{12}')", ppmdapprover.displayName, ppmdapprover.displayName, ppmdapprover.displayName, true, ppmdapprover.id, "Domain", "Office", true, data.submittedby, date, data.submittedby, date, "title");

                                command = new OdbcCommand(query1, connection);
                                rowAffected = command.ExecuteNonQuery();
                            }

                            //Adding user to user role table (user and engagement details)

                            query1 = string.Format("select id from mna.users where EmailID='{0}'", ppmdapprover.id);
                            command = new OdbcCommand(query1, connection);
                            reader = command.ExecuteReader();
                            userId = 0;
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    userId = reader.GetInt32(0);
                                }
                            }

                            if (userId > 0)
                            {
                                query1 = String.Format("insert into mna.user_roles(UserId," +
                                "Engagement_Id," +
                                "Role_Id," +
                                "Can_Write," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "IsActive) values({0},{1},{2},{3},'{4}',{5},'{6}',{7},{8})", userId, latestEngagementId, 1, ppmdapprover.writePermission, data.submittedby, date, data.submittedby, date, true);

                                command = new OdbcCommand(query1, connection);

                                rowAffected = command.ExecuteNonQuery();

                            }
                        }



                    }

                    //adding team member to user table
                    if ((data.engagementteams != null) && (data.engagementteams.Any()))
                    {
                        foreach (var engagementteam in data.engagementteams)
                        {
                            AddAccessUser(engagementteam.id, data.submittedby);
                            query1 = String.Format("select count(*) from mna.users where EmailId = '{0}'", engagementteam.id);
                            command = new OdbcCommand(query1, connection);

                            int count = 0;
                            count = Convert.ToInt32(command.ExecuteScalar());

                            if (count == 0)
                            {
                                query1 = String.Format("insert into mna.users(Alias," +
                                "Last_Name," +
                                "First_Name," +
                                "IsPPMD," +
                                "EmailID," +
                                "Domain," +
                                "Office," +
                                "IsActive," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "Title) values('{0}','{1}','{2}',{3},'{4}','{5}','{6}','{7}','{8}',{9},'{10}',{11},'{12}')", engagementteam.displayName, engagementteam.displayName, engagementteam.displayName, false, engagementteam.id, "Domain", "Office", true, data.submittedby, date, data.submittedby, date, "title");

                                command = new OdbcCommand(query1, connection);
                                rowAffected = command.ExecuteNonQuery();
                            }

                            //Adding user to user role table (user and engagement details)

                            query1 = string.Format("select id from mna.users where EmailID='{0}'", engagementteam.id);
                            command = new OdbcCommand(query1, connection);
                            reader = command.ExecuteReader();
                            userId = 0;
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    userId = reader.GetInt32(0);
                                }
                            }

                            if (userId > 0)
                            {
                                query1 = String.Format("insert into mna.user_roles(UserId," +
                                "Engagement_Id," +
                                "Role_Id," +
                                "Can_Write," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "IsActive) values({0},{1},{2},{3},'{4}',{5},'{6}',{7},{8})", userId, latestEngagementId, 2, engagementteam.writePermission, data.submittedby, date, data.submittedby, date, true);

                                command = new OdbcCommand(query1, connection);

                                rowAffected = command.ExecuteNonQuery();

                            }
                        }



                    }

                    // adding client menmber to the table
                    if ((data.clientTeams != null) && (data.clientTeams.Any()))
                    {
                        foreach (var clienntteam in data.clientTeams)
                        {

                            query1 = String.Format("select count(*) from mna.users where EmailId = '{0}'", clienntteam.email);
                            command = new OdbcCommand(query1, connection);

                            int count = 0;
                            count = Convert.ToInt32(command.ExecuteScalar());
                            string fullName = "" + clienntteam.firstName + " " + clienntteam.lastName;

                            if (count == 0)
                            {
                                query1 = String.Format("insert into mna.users(Alias," +
                                "Last_Name," +
                                "First_Name," +
                                "IsPPMD," +
                                "EmailID," +
                                "Domain," +
                                "Office," +
                                "IsActive," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "Title) values('{0}','{1}','{2}',{3},'{4}','{5}','{6}','{7}','{8}',{9},'{10}',{11},'{12}')", fullName, clienntteam.lastName, clienntteam.firstName, false, clienntteam.email, "Domain", "Office", true, data.submittedby, date, data.submittedby, date, "title");

                                command = new OdbcCommand(query1, connection);
                                rowAffected = command.ExecuteNonQuery();
                            }

                            //Adding user to user role table (user and engagement details)

                            query1 = string.Format("select id from mna.users where EmailID='{0}'", clienntteam.email);
                            command = new OdbcCommand(query1, connection);
                            reader = command.ExecuteReader();
                            userId = 0;
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    userId = reader.GetInt32(0);
                                }
                            }

                            if (userId > 0)
                            {
                                query1 = String.Format("insert into mna.user_roles(UserId," +
                                "Engagement_Id," +
                                "Role_Id," +
                                "Can_Write," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "IsActive) values({0},{1},{2},{3},'{4}',{5},'{6}',{7},{8})", userId, latestEngagementId, 3, false, data.submittedby, date, data.submittedby, date, true);

                                command = new OdbcCommand(query1, connection);

                                rowAffected = command.ExecuteNonQuery();

                            }
                        }



                    }

                    return latestEngagementId;

                }
                catch (Exception e)
                {
                    _logger.LogError("SaveEngagementDetails : " + e.Message);
                    return -1;
                }
                //query2=
            }
        }
        public List<EngagementPortfolio> GetOfferingPortfolio()
        {
            List<EngagementPortfolio> PortfolioList = new List<EngagementPortfolio>();

            string connectionString = GetODBCConnection();

            string resp = "";
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "select * from mna.offering_portfolio";
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    OdbcDataReader reader = command.ExecuteReader();
                    DataTable dt = new DataTable();
                    dt.Load(reader);
                    PortfolioList = (from DataRow dr in dt.Rows
                                     select new EngagementPortfolio()
                                     {
                                         Id = Convert.ToInt32(dr["Id"]),
                                         Name = dr["Name"].ToString(),
                                         CreatedUser = dr["CreatedUser"].ToString(),
                                         CreatedDate = Convert.ToDateTime(dr["CreatedDate"]),
                                         ModifiedUser = dr["ModifiedUser"].ToString(),
                                         ModifiedDate = Convert.ToDateTime(dr["ModifiedDate"]),
                                         IsActive = Convert.ToBoolean(dr["IsActive"])
                                     }
                             ).ToList();


                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError("GetOfferingPortfolio : " + ex.Message);
                    resp = ex.Message;
                }
            }
            return PortfolioList;
        }
        public string getReturnForReworkComment(int? engagementId)
        {
            string comment = string.Empty;
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = String.Format("select comment from mna.return_rework_comments where Engagement_Id  = {0} order by Id desc", engagementId);
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    OdbcDataReader reader = command.ExecuteReader();
                    DataTable dt = new DataTable();
                    dt.Load(reader);
                    DataRow dr = dt.Rows[0];
                    comment = dr[0].ToString();

                }
                catch (Exception ex)
                {
                    _logger.LogError("getReturnForReworkComment : " + ex.Message);
                    return string.Empty;
                }
                return comment;
            }
        }
        public int SaveFolderDetails(string FolderURL, int? EngagementID,string FolderName)
        {
            int records = 0;
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "Insert into mna.adls_details(Engagement_id,Folder_URL,CreatedUser,CreatedDate,ModifiedUser,ModifiedDate,IsActive,Folder_Name)" +
                    "values(" + EngagementID + "," + "'" + FolderURL + "','pvenkatasatyanara@deloitte.com',getdate(),'pvenkatasatyanara@deloitte.com',getdate(),true,"+"'"+FolderName+"'"+")";
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    records = command.ExecuteNonQuery();

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError("SaveFolderDetails : " + ex.Message);
                }
            }
            return records;

        }

        public SGGroupDetails GetSGGroup()
        {

            string connectionString = GetODBCConnection();
            SGGroupDetails sgGroupDetails = new SGGroupDetails();

            string resp = "";
            string sqlQuery;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "select  * from mna.sggroupdetails where Is_inuse=false order by Id limit 1";
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    OdbcDataReader reader = command.ExecuteReader();
                    DataTable dt = new DataTable();
                    dt.Load(reader);
                    foreach (DataRow dr in dt.Rows)
                    {
                        sgGroupDetails.Id = Convert.ToInt32(dr["Id"]);
                        sgGroupDetails.GroupName = dr["GroupName"].ToString();
                        sgGroupDetails.GroupId = dr["GroupID"].ToString();
                        sgGroupDetails.Is_Inuse = Convert.ToBoolean(dr["Is_Inuse"]);
                    }


                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError("GetSGGroup : " + ex.Message);
                    resp = ex.Message;
                }
                return sgGroupDetails;
            }
        }

        public int UpdateSGGroupDetails(int? EngagementID, string Groupname)
        {
            int records = 0;
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "update mna.sggroupdetails set Is_Inuse=true,Engagement_Id=" + EngagementID + " where GroupName='" + Groupname + "'";
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    records = command.ExecuteNonQuery();

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError("UpdateSGGroupDetails : " + ex.Message);
                }
            }
            return records;

        }
        public DatabricksLoginInfo GetDatabricksLoginInfo(int EngagementId)
        {
            DatabricksLoginInfo databricksLoginInfo = new DatabricksLoginInfo();
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = String.Format("select Accountname,PAT from mna.serviceaccountdetails where Engagement_Id = {0} limit 1",EngagementId);
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    OdbcDataReader reader = command.ExecuteReader();
                    DataTable dt = new DataTable();
                    dt.Load(reader);
                    var Host = _config.GetValue<string>("DatabricksConnection:Host");
                    var hpptpath= _config.GetValue<string>("DatabricksConnection:HTTPPath");
                    foreach (DataRow dr in dt.Rows)
                    {
                        databricksLoginInfo.accountName = dr["Accountname"].ToString();
                        databricksLoginInfo.pat = dr["PAT"].ToString();
                        databricksLoginInfo.hostname = Host;
                        databricksLoginInfo.httppath = hpptpath;
                    }
                    command.Dispose();
                    return databricksLoginInfo;
                }
                catch (Exception ex)
                {
                    _logger.LogError("GetDatabricksLoginInfo : " + ex.Message);
                    return null;
                }
            }
        }
        public bool CheckEngagementAccess(int EngagementId, string EmailId)
        {

            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            int userid = 0;
            //string accessCheckQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = String.Format("select ur.userid " +
                            " from mna.user_roles ur inner join mna.users u on u.id = ur.userid" +
                            " inner join mna.roles r on r.id = ur.role_id" +
                            " where u.emailId = '{0}' and ur.Engagement_id = {1} and ur.Role_Id in (1,2)", EmailId, EngagementId);
                //accessCheckQuery = String.Format("select count(ur.Can_Write) from mna.user_roles ur inner join mna.users u on u.Id = ur.UserId where u.EmailID = '{0}' AND ur.Engagement_Id = {1}", EmailId, EngagementId);
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                //OdbcCommand accessCheckCommand = new OdbcCommand(accessCheckQuery, connection);
                connection.Open();
                try
                {
                    OdbcDataReader reader = command.ExecuteReader();
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            userid = reader.GetInt32(0);
                        }
                    }
                    if (userid > 0)
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }


                }
                catch (Exception ex)
                {
                    _logger.LogError("CheckEngagementAccess : " + ex.Message);
                    return false;
                }
            }
        }
        public List<SecurityMatrix> GetSecurityMatrix()
        {
            List<SecurityMatrix> securityMatrixList = new List<SecurityMatrix>();
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = ("select Engagement_Status_Id,PPMDApprovers,EngagementTeams from mna.securitymatrix");
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    OdbcDataReader reader = command.ExecuteReader();
                    DataTable dt = new DataTable();
                    dt.Load(reader);
                    foreach (DataRow dr in dt.Rows)
                    {
                        SecurityMatrix securityMatrix = new SecurityMatrix();
                        securityMatrix.engagementStatusId = Convert.ToInt32(dr["Engagement_Status_Id"]);
                        securityMatrix.ppmdApprovers = Convert.ToBoolean(dr["PPMDApprovers"]);
                        securityMatrix.engagementTeams = Convert.ToBoolean(dr["EngagementTeams"]);
                        securityMatrixList.Add(securityMatrix);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError("GetSecurityMatrix : " + ex.Message);
                    return null;
                }
                return securityMatrixList;
            }
        }
        public EngagementData GetEngagementData(int EngagementId)
        {
            if (EngagementId == 0)
                throw new Exception("Id must be greater than zero");
            EngagementData engagementData = new EngagementData();
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            string EngagementQuery = "";
            string DatasetQuery = "";
            string PausedQuery = "";       
            string filename = "";            

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {

                sqlQuery = "select users.Id,users.First_Name,users.Last_Name,users.EmailID,user_roles.Role_Id,roles.Role_name,user_roles.Can_Write from mna.users users inner join mna.user_roles user_roles on users.Id=user_roles.UserId" +
                            " inner join mna.roles roles on roles.Id = user_roles.Role_Id" +
                            " where user_roles.Engagement_Id =" + EngagementId;

              EngagementQuery = "select state.state_Name,ed.modifiedDate, ed.Engagement_Name,ed.Offering_Portfolio_Id,portfolio.Name,ed.WBS_Code,ed.CreatedDate,ed.CreatedUser,ed.Is_Engagement_Buy_Side,ed.Current_state,ed.Previous_state,ad.folder_url,ad.folder_name" +
                                  " from mna.engagement_details ed join  mna.engagement_state state on state.id=ed.current_state left join mna.adls_details ad on ed.id=ad.engagement_id join mna.offering_portfolio portfolio on ed.Offering_Portfolio_Id=portfolio.id" + " where ed.Id=" + EngagementId;

                DatasetQuery = string.Format("select dd.dataset_name, dd.tablename, dd.uploaded_to_databricks from mna.dataset_details dd where dd.engagement_id={0} and IsActive=true", EngagementId);


                PausedQuery = string.Format("select client_access.is_paused from mna.client_access where client_access.Engagement_Id={0}",EngagementId);


                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                OdbcCommand Engagementcommand = new OdbcCommand(EngagementQuery, connection);
                OdbcCommand datasetCommand = new OdbcCommand(DatasetQuery, connection);               
                OdbcCommand isPauseCheckCommand = new OdbcCommand(PausedQuery,connection);
                connection.Open();
                try
                {

                    List<UserModelApprover> PPMDApprovers = new List<UserModelApprover>();
                    List<UserModelApprover> EngagementMembers = new List<UserModelApprover>();
                    List<Dataset> Datasets = new List<Dataset>();
                    List<UserModel> ClientTeam = new List<UserModel>();
                    OdbcDataReader reader = command.ExecuteReader();
                    OdbcDataReader Engagementreader = Engagementcommand.ExecuteReader();
                    OdbcDataReader datasetreader = datasetCommand.ExecuteReader();
                    OdbcDataReader isPausedReader = isPauseCheckCommand.ExecuteReader();
                    DataTable dt = new DataTable();
                    DataTable EngDT = new DataTable();
                    DataTable dsDT = new DataTable();
                    DataTable pauseDT = new DataTable();
                    EngDT.Load(Engagementreader);
                    dt.Load(reader);
                    dsDT.Load(datasetreader);
                    pauseDT.Load(isPausedReader);

                    foreach (DataRow dr in dt.Rows)
                    {
                        UserModel model = new UserModel();
                        UserModelApprover modelApprover = new UserModelApprover();

                        if (dr["Role_name"].ToString() == "PPMD Approver")
                        {
                            //modelApprover.UserId = Convert.ToInt32(dr["Id"]);
                            modelApprover.givenName = dr["First_Name"].ToString();
                            modelApprover.displayName = dr["Last_Name"].ToString();
                            modelApprover.id = dr["EmailID"].ToString();
                            modelApprover.readPermission = Convert.ToBoolean(dr["Can_Write"]) ? false : true;
                            modelApprover.writePermission = Convert.ToBoolean(dr["Can_Write"]);
                            PPMDApprovers.Add(modelApprover);
                        }
                        else if (dr["Role_name"].ToString() == "Engagement Member")
                        {
                            //modelApprover.UserId = Convert.ToInt32(dr["Id"]);
                            modelApprover.givenName = dr["First_Name"].ToString();
                            modelApprover.displayName = dr["Last_Name"].ToString();
                            modelApprover.id = dr["EmailID"].ToString();
                            modelApprover.readPermission = Convert.ToBoolean(dr["Can_Write"]) ? false : true;
                            modelApprover.writePermission = Convert.ToBoolean(dr["Can_Write"]);
                            EngagementMembers.Add(modelApprover);
                        }
                        else if (dr["Role_name"].ToString() == "Client Team")
                        {
                            model.UserId = Convert.ToInt32(dr["Id"]);
                            model.FirstName = dr["First_Name"].ToString();
                            model.LastName = dr["Last_Name"].ToString();
                            model.Email = dr["EmailID"].ToString();
                            model.canWrite = Convert.ToBoolean(dr["Can_Write"]);
                            ClientTeam.Add(model);
                        }
                    }
                    foreach (DataRow dr in dsDT.Rows)
                    {
                        Dataset modelDataset = new Dataset();
                        modelDataset.dataset_name = dr["dataset_name"].ToString();
                        modelDataset.tablename = dr["tablename"].ToString();
                        modelDataset.uploaded_to_databricks = Convert.ToBoolean(dr["uploaded_to_databricks"]);
                        Datasets.Add(modelDataset);
                    }                    
                    foreach (DataRow dr in EngDT.Rows)
                    {
                        engagementData.id = EngagementId;
                        engagementData.createdDate = Convert.ToDateTime(dr["CreatedDate"]);
                        engagementData.modifiedDate = Convert.ToDateTime(dr["ModifiedDate"]);
                        engagementData.submittedBy = dr["CreatedUser"].ToString();
                        engagementData.engagementName = dr["Engagement_Name"].ToString();
                        engagementData.offeringPortfolio = Convert.ToInt32(dr["Offering_Portfolio_Id"]);
                        engagementData.wbsCode = dr["WBS_Code"].ToString();
                        engagementData.offeringPortfolioName = dr["Name"].ToString();
                        engagementData.adlsFolderUrl = dr["folder_url"].ToString();
                        engagementData.adlsFolderName = dr["folder_name"].ToString();
                        engagementData.engagementisBuySide = Convert.ToBoolean(dr["Is_Engagement_Buy_Side"]);
                        engagementData.currentStatus = Convert.ToInt32(dr["Current_state"]);
                        engagementData.previousStatus = Convert.ToInt32(dr["Previous_state"]);
                        engagementData.userhasWritePermission = true;
                        engagementData.tableauURL = "";
                        engagementData.dataSets = Datasets;
                        engagementData.ppmdApprovers = PPMDApprovers;
                        engagementData.engagementTeams = EngagementMembers;
                        engagementData.clientTeams = ClientTeam;
                        engagementData.stateName = dr["state_Name"].ToString();
                    }

                    foreach (DataRow dr in pauseDT.Rows)
                    {
                        engagementData.is_Paused = Convert.ToBoolean(dr["is_paused"]);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError("GetEngagementData : " + ex.Message);
                }
                return engagementData;
            }
        }

        public bool UpdateActivityLog(EngagementData oldValue,EngagementDetails latestValue)
        {
            //EngagementData engagementData = new EngagementData();
            string connectionString = GetODBCConnection();
            string updateQuery = "";
            string date = "getdate()";
            int rowAffected;
            OdbcCommand command;
            //oldValue = GetEngagementData(latestValue.id ?? 0);
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                connection.Open();
                try
                { 
                    //for updating the engagement name
                    if (!oldValue.engagementName.Equals(latestValue.engagementname))
                    {
                        updateQuery = string.Format("insert into mna.activity_log(Engagement_Id, " +
                            "FieldName, " +
                            "Old_Value, " +
                            "New_Value, " +
                            "Modified_User," +
                            "Modeified_Date)values({0},'{1}','{2}','{3}','{4}',{5})", latestValue.id, "Engagement Name", oldValue.engagementName, latestValue.engagementname, latestValue.submittedby, date);
                         command = new OdbcCommand(updateQuery, connection);
                        rowAffected = command.ExecuteNonQuery();
                    }
                    //for updating the adls folder name
                    if (!oldValue.adlsFolderName.Equals(latestValue.adlsfoldername))
                    {
                        updateQuery = string.Format("insert into mna.activity_log(Engagement_Id, " +
                            "FieldName, " +
                            "Old_Value, " +
                            "New_Value, " +
                            "Modified_User," +
                            "Modeified_Date)values({0},'{1}','{2}','{3}','{4}',{5})", latestValue.id, "Adls Folder Name", oldValue.adlsFolderName, latestValue.adlsfoldername, latestValue.submittedby, date);
                        command = new OdbcCommand(updateQuery, connection);
                        rowAffected = command.ExecuteNonQuery();
                    }
                    //for updating the adls folder url
                    if (!oldValue.adlsFolderUrl.Equals(latestValue.adlsfolderurl))
                    {
                        updateQuery = string.Format("insert into mna.activity_log(Engagement_Id, " +
                            "FieldName, " +
                            "Old_Value, " +
                            "New_Value, " +
                            "Modified_User," +
                            "Modeified_Date)values({0},'{1}','{2}','{3}','{4}',{5})", latestValue.id, "Adls Folder Url", oldValue.adlsFolderUrl, latestValue.adlsfolderurl, latestValue.submittedby, date);
                        command = new OdbcCommand(updateQuery, connection);
                        rowAffected = command.ExecuteNonQuery();
                    }
                    //for updating the wbs code
                    if (!oldValue.wbsCode.Equals(latestValue.wbscode))
                    {
                        updateQuery = string.Format("insert into mna.activity_log(Engagement_Id, " +
                            "FieldName, " +
                            "Old_Value, " +
                            "New_Value, " +
                            "Modified_User," +
                            "Modeified_Date)values({0},'{1}','{2}','{3}','{4}',{5})", latestValue.id, "Wbs Code", oldValue.wbsCode, latestValue.wbscode, latestValue.submittedby, date);
                        command = new OdbcCommand(updateQuery, connection);
                        rowAffected = command.ExecuteNonQuery();
                    }
                    //for updating the engagement buy side
                    if (oldValue.engagementisBuySide != (latestValue.engagementisbuyside ?? false))
                    {
                        updateQuery = string.Format("insert into mna.activity_log(Engagement_Id, " +
                            "FieldName, " +
                            "Old_Value, " +
                            "New_Value, " +
                            "Modified_User," +
                            "Modeified_Date)values({0},'{1}','{2}','{3}','{4}',{5})", latestValue.id, "Engagement Buy Side", oldValue.engagementisBuySide ? "true" : "false", latestValue.engagementisbuyside ?? false ? "true" : "false", latestValue.submittedby, date);
                        command = new OdbcCommand(updateQuery, connection);
                        rowAffected = command.ExecuteNonQuery();
                    }
                   //update ppmd read access
                    var updateReadAccess = oldValue.ppmdApprovers.Where(o => latestValue.ppmdapprovers.Any(n => n.id.Equals(o.id) && n.readPermission != o.readPermission)).ToList();
                    if (updateReadAccess.Count > 0)
                    {
                       // string oldReadAccessUpdate = "";
                        foreach (var ppmd in oldValue.ppmdApprovers)
                        {
                            updateQuery = string.Format("insert into mna.activity_log(Engagement_Id, " +
                            "FieldName, " +
                            "Old_Value, " +
                            "New_Value, " +
                            "Modified_User," +
                            "Modeified_Date)values({0},'{1}','{2}','{3}','{4}',{5})", latestValue.id, "PPMD Approver Read Access Update for user " + ppmd.id,ppmd.readPermission? "true":"false", !ppmd.readPermission ? "true" : "false", latestValue.submittedby, date);
                            command = new OdbcCommand(updateQuery, connection);
                            rowAffected = command.ExecuteNonQuery();

                        }
                    }
                    //update  ppmd write access
                    var updateWriteAccess = oldValue.ppmdApprovers.Where(o => latestValue.ppmdapprovers.Any(n => n.id.Equals(o.id) && n.writePermission != o.writePermission)).ToList();
                    if (updateWriteAccess.Count > 0)
                    {
                        foreach (var ppmd in oldValue.ppmdApprovers)
                        {
                            updateQuery = string.Format("insert into mna.activity_log(Engagement_Id, " +
                            "FieldName, " +
                            "Old_Value, " +
                            "New_Value, " +
                            "Modified_User," +
                            "Modeified_Date)values({0},'{1}','{2}','{3}','{4}',{5})", latestValue.id, "PPMD Approver Write Access Update for user " + ppmd.id, ppmd.writePermission ? "true" : "false", !ppmd.writePermission ? "true" : "false", latestValue.submittedby, date);
                            command = new OdbcCommand(updateQuery, connection);
                            rowAffected = command.ExecuteNonQuery();

                        }
                    }
                    // update ppmd add user or the remove user
                    //check for ppmd user remove
                    if (oldValue.ppmdApprovers.Count >= latestValue.ppmdapprovers?.Count)
                    {
                        var diffPPMDApprover = oldValue.ppmdApprovers.Where(o => !latestValue.ppmdapprovers.Any(n => n.id.Equals(o.id))).ToList();
                        if (diffPPMDApprover.Count > 0)
                        {
                            // string oldPPMDApprover='', newPPMDApprover='';
                            string oldPPMD = "";
                            string newPPMD = "";
                            foreach (var ppmd in oldValue.ppmdApprovers.Select(u => u.id).ToList())
                            {
                                oldPPMD += ppmd + " ,";
                            }
                            oldPPMD.TrimEnd(',');

                            foreach (var ppmd in latestValue.ppmdapprovers.Select(u => u.id).ToList())
                            {
                                newPPMD += ppmd + " ,";
                            }
                            newPPMD.TrimEnd(',');
                            updateQuery = string.Format("insert into mna.activity_log(Engagement_Id, " +
                                "FieldName, " +
                                "Old_Value, " +
                                "New_Value, " +
                                "Modified_User," +
                                "Modeified_Date)values({0},'{1}','{2}','{3}','{4}',{5})", latestValue.id, "PPMD Approver User Removal", oldPPMD, newPPMD, latestValue.submittedby, date);
                            command = new OdbcCommand(updateQuery, connection);
                            rowAffected = command.ExecuteNonQuery();
                        }
                    }
                    else if (oldValue.ppmdApprovers.Count <= latestValue.ppmdapprovers?.Count)
                    {
                        var diffPPMDApprover = latestValue.ppmdapprovers.Where(o => !oldValue.ppmdApprovers.Any(n => n.id.Equals(o.id))).ToList();
                        if (diffPPMDApprover.Count > 0)
                        {
                            // string oldPPMDApprover='', newPPMDApprover='';
                            string oldPPMD = "";
                            string newPPMD = "";
                            foreach (var ppmd in oldValue.ppmdApprovers.Select(u => u.id).ToList())
                            {
                                oldPPMD += ppmd + " ,";
                            }
                            oldPPMD.TrimEnd(',');

                            foreach (var ppmd in latestValue.ppmdapprovers.Select(u => u.id).ToList())
                            {
                                newPPMD += ppmd + " ,";
                            }
                            newPPMD.TrimEnd(',');
                            updateQuery = string.Format("insert into mna.activity_log(Engagement_Id, " +
                                "FieldName, " +
                                "Old_Value, " +
                                "New_Value, " +
                                "Modified_User," +
                                "Modeified_Date)values({0},'{1}','{2}','{3}','{4}',{5})", latestValue.id, "PPMD Approver User Added", oldPPMD, newPPMD, latestValue.submittedby, date);
                            command = new OdbcCommand(updateQuery, connection);
                            rowAffected = command.ExecuteNonQuery();
                        }
                    }
                    //update engagement team read access
                     updateReadAccess = oldValue.engagementTeams.Where(o => latestValue.engagementteams.Any(n => n.id.Equals(o.id) && n.readPermission != o.readPermission)).ToList();
                    if (updateReadAccess.Count > 0)
                    {
                        // string oldReadAccessUpdate = "";
                        foreach (var engteam in oldValue.engagementTeams)
                        {
                            updateQuery = string.Format("insert into mna.activity_log(Engagement_Id, " +
                            "FieldName, " +
                            "Old_Value, " +
                            "New_Value, " +
                            "Modified_User," +
                            "Modeified_Date)values({0},'{1}','{2}','{3}','{4}',{5})", latestValue.id, "Engaggement Team Read Access Update for user " + engteam.id, engteam.readPermission ? "true" : "false", !engteam.readPermission ? "true" : "false", latestValue.submittedby, date);
                            command = new OdbcCommand(updateQuery, connection);
                            rowAffected = command.ExecuteNonQuery();

                        }
                    }
                    //update  engagement team write access
                     updateWriteAccess = oldValue.engagementTeams.Where(o => latestValue.engagementteams.Any(n => n.id.Equals(o.id) && n.writePermission != o.writePermission)).ToList();
                    if (updateWriteAccess.Count > 0)
                    {
                        foreach (var engTeam in oldValue.engagementTeams)
                        {
                            updateQuery = string.Format("insert into mna.activity_log(Engagement_Id, " +
                            "FieldName, " +
                            "Old_Value, " +
                            "New_Value, " +
                            "Modified_User," +
                            "Modeified_Date)values({0},'{1}','{2}','{3}','{4}',{5})", latestValue.id, "Engagement Team Write Access Update for user " + engTeam.id, engTeam.writePermission ? "true" : "false", !engTeam.writePermission ? "true" : "false", latestValue.submittedby, date);
                            command = new OdbcCommand(updateQuery, connection);
                            rowAffected = command.ExecuteNonQuery();

                        }
                    }
                    // update Engagement Team  add user or the remove user
                    //check for ppmd user remove
                    if (oldValue.engagementTeams.Count >= latestValue.engagementteams?.Count)
                    {
                        var diffEngagementTeam = oldValue.engagementTeams.Where(o => !latestValue.engagementteams.Any(n => n.id.Equals(o.id))).ToList();
                        if (diffEngagementTeam.Count > 0)
                        {
                            // string oldPPMDApprover='', newPPMDApprover='';
                            string oldEngagementTeam = "";
                            string newEngagementTeam = "";
                            foreach (var engTeam in oldValue.engagementTeams.Select(u => u.id).ToList())
                            {
                                oldEngagementTeam += engTeam + " ,";
                            }
                            oldEngagementTeam.TrimEnd(',');

                            foreach (var engTeam in latestValue.engagementteams.Select(u => u.id).ToList())
                            {
                                newEngagementTeam += engTeam + " ,";
                            }
                            newEngagementTeam.TrimEnd(',');
                            updateQuery = string.Format("insert into mna.activity_log(Engagement_Id, " +
                                "FieldName, " +
                                "Old_Value, " +
                                "New_Value, " +
                                "Modified_User," +
                                "Modeified_Date)values({0},'{1}','{2}','{3}','{4}',{5})", latestValue.id, "Engagement Team User Removal", oldEngagementTeam, newEngagementTeam, latestValue.submittedby, date);
                            command = new OdbcCommand(updateQuery, connection);
                            rowAffected = command.ExecuteNonQuery();
                        }
                    }
                    else if (oldValue.engagementTeams.Count <= latestValue.engagementteams?.Count)
                    {
                        var diffEngagementTeam = latestValue.engagementteams.Where(o => !oldValue.engagementTeams.Any(n => n.id.Equals(o.id))).ToList();
                        if (diffEngagementTeam.Count > 0)
                        {
                            // string oldPPMDApprover='', newPPMDApprover='';
                            string oldEngagementTeam = "";
                            string newEngagementTeam = "";
                            foreach (var engTeam in oldValue.engagementTeams.Select(u => u.id).ToList())
                            {
                                oldEngagementTeam += engTeam + " ,";
                            }
                            oldEngagementTeam.TrimEnd(',');

                            foreach (var engTeam in latestValue.engagementteams.Select(u => u.id).ToList())
                            {
                                newEngagementTeam += engTeam + " ,";
                            }
                            newEngagementTeam.TrimEnd(',');
                            updateQuery = string.Format("insert into mna.activity_log(Engagement_Id, " +
                                "FieldName, " +
                                "Old_Value, " +
                                "New_Value, " +
                                "Modified_User," +
                                "Modeified_Date)values({0},'{1}','{2}','{3}','{4}',{5})", latestValue.id, "Engagement Team User Added", oldEngagementTeam, newEngagementTeam, latestValue.submittedby, date);
                            command = new OdbcCommand(updateQuery, connection);
                            rowAffected = command.ExecuteNonQuery();
                        }
                    }
                    


                }
                catch (Exception ex)
                {
                    _logger.LogError("UpdateActivityLog : " + ex.Message);
                    return false;
                }
                return true;
            }
        }

        public List<DashboardData> GetAllEngagements(string EmailId)
        {
            List<DashboardData> DashboardData = new List<DashboardData>();
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "select distinct ED.id,ED.Engagement_Name,OP.Name,ED.WBS_Code, ES.State_Name,'' as ADLS_Folder,ED.Is_Engagement_Buy_Side" +
                            " from mna.engagement_details ED inner join mna.offering_portfolio OP on ED.Offering_Portfolio_Id = OP.Id" +
                            " inner join mna.engagement_state ES on ED.Current_state = ES.Id" +
                            " inner join mna.user_roles UR on ur.Engagement_Id = ED.Id" +
                            " inner join mna.users u on U.Id = UR.UserId" +
                            " where u.EmailID = '" + EmailId + "'" +
                            " and UR.Role_Id in (1, 2)" +
                            " order by ED.ID";
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {


                    OdbcDataReader reader = command.ExecuteReader();
                    DataTable dt = new DataTable();
                    dt.Load(reader);

                    foreach (DataRow dr in dt.Rows)
                    {
                        DashboardData dashboardData = new DashboardData();

                        dashboardData.EngagementID = Convert.ToInt32(dr["id"]);
                        dashboardData.EngagementName = dr["Engagement_Name"].ToString();
                        dashboardData.Offering_Portfolio = dr["Name"].ToString();
                        dashboardData.wbscode = dr["WBS_Code"].ToString();
                        dashboardData.Engagement_Status = dr["State_Name"].ToString();
                        dashboardData.ADLS_Folder = dr["ADLS_Folder"].ToString();
                        dashboardData.Engagement_Buy_Side = Convert.ToBoolean(dr["Is_Engagement_Buy_Side"]);
                        dashboardData.statuschangeduration = 0;
                        DashboardData.Add(dashboardData);

                    }

                }
                catch (Exception ex)
                {
                    _logger.LogError("GetAllEngagements : " + ex.Message);

                }
                return DashboardData;
            }
        }

        public List<Audit> GetAuditDetails(int EngagementId)
        {
            List<Audit> lstAudit = new List<Audit>();
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "select ad.Engagement_Id,ad.Audit_type,ad.isactive,ad.createduser,ad.createddate" +
                    ",ad.modifieduser,ad.modifieddate,ad.old_value,ad.new_value" +
                            " from mna.audit ad" +
                            " where ad.Engagement_Id =" + EngagementId;
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    OdbcDataReader reader = command.ExecuteReader();
                    DataTable dt = new DataTable();
                    dt.Load(reader);

                    foreach (DataRow dr in dt.Rows)
                    {
                        Audit auditData = new Audit();

                        auditData.engagement_Id = Convert.ToInt32(dr["Engagement_Id"]);
                        auditData.isActive = Convert.ToBoolean(dr["isActive"]);
                        auditData.auditType = Convert.ToString(dr["Audit_type"]);
                        auditData.oldValue = Convert.ToString(dr["old_value"]);
                        auditData.newValue = Convert.ToString(dr["new_value"]);
                        auditData.createdUser = Convert.ToString(dr["createduser"]);
                        auditData.createdDate = Convert.ToDateTime(dr["createddate"]);
                        auditData.modifiedUser = Convert.ToString(dr["modifieduser"]);
                        auditData.modifiedDate = Convert.ToDateTime(dr["modifieddate"]);

                        lstAudit.Add(auditData);

                    }

                }
                catch (Exception ex)
                {
                    _logger.LogError("GetAllEngagements : " + ex.Message);

                }
                return lstAudit;
            }
        }
        public bool AddComments(AddComment addComment)
        {

            bool status = false;
            string connectionString = GetODBCConnection();

            string resp = "";
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = string.Format("insert into mna.Return_Rework_Comments(Engagement_Id,Comment) values({0},'{1}')", addComment.Engagement_Id, addComment.Comment);
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    command.ExecuteNonQuery();
                    status = true;

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError("AddComments : " + ex.Message);
                    resp = ex.Message;
                }
            }
            return status;
        }

        public List<Dataset> AddDatasets(List<string> FilesList, int EngagementId, string username)
        {
            string connectionString = GetODBCConnection();
            string ADLSId_query = "";
            string Dataset_query = "";
            string InsertQuery = "";
            string UpdateQuery = "";
            string filename = "";
            string DatasetActivity_Query = "";
            List<string> SavedDataset = new List<string>();
            List<Dataset> Datasets = new List<Dataset>();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                ADLSId_query = string.Format("select ad.Id from mna.adls_details ad where ad.engagement_id={0}", EngagementId);

                OdbcCommand ADLS_command = new OdbcCommand(ADLSId_query, connection);
                connection.Open();
                try
                {
                    OdbcDataReader reader = ADLS_command.ExecuteReader();
                    DataTable dataTable = new DataTable();
                    dataTable.Load(reader);
                    DataRow drow = dataTable.Rows[0];
                    int ADLS_Id = Convert.ToInt32(drow[0]);

                    Dataset_query = string.Format("select dd.dataset_name from mna.dataset_details dd where dd.engagement_id={0} and IsActive=true", EngagementId);
                    OdbcCommand dataset_command = new OdbcCommand(Dataset_query, connection);
                    OdbcDataReader datasetreader = dataset_command.ExecuteReader();
                    DataTable dddataTable = new DataTable();
                    dddataTable.Load(datasetreader);
                    foreach (DataRow dr in dddataTable.Rows)
                    {
                        filename = dr["dataset_name"].ToString();
                        SavedDataset.Add(filename);
                    }
                    var firstNotSecond = FilesList.Except(SavedDataset).ToList();
                    var secondNotFirst = SavedDataset.Except(FilesList).ToList();


                    foreach (string file in firstNotSecond)
                    {
                        string message = "Uploaded to ADLS folder";
                        AddDatasetLog(EngagementId, file, message, username);
                        InsertQuery = String.Format("insert into mna.dataset_details(Engagement_Id," +
                                "ADLS_Id," +
                                "Dataset_Name," +
                                "IsActive," +
                                "CreatedUser," +
                                "CreatedDate," +
                                "ModifiedUser," +
                                "ModifiedDate," +
                                "Tablename," +
                                "Uploaded_To_Databricks" +
                                ") values({0},{1},'{2}',{3},'{4}',{5},'{6}',{7},'{8}',{9})", EngagementId, ADLS_Id, file, true, username, "getdate()", username, "getdate()", string.Empty, false);
                        OdbcCommand insertCommand = new OdbcCommand(InsertQuery, connection);
                        insertCommand.ExecuteNonQuery();
                        
                    }
                    foreach (string file in secondNotFirst)
                    {
                        string message = "Deleted from ADLS folder";
                        AddDatasetLog(EngagementId, file, message, username);
                        UpdateQuery = String.Format("update mna.dataset_details set IsActive=false where Engagement_Id={0} and ADLS_Id={1} and Dataset_Name='{2}'",
                            EngagementId, ADLS_Id, file);
                        OdbcCommand updateCommand = new OdbcCommand(UpdateQuery, connection);
                        updateCommand.ExecuteNonQuery();
                       
                    }
                    string DatasetQuery = string.Format("select dd.dataset_name, dd.tablename, dd.uploaded_to_databricks from mna.dataset_details dd where dd.engagement_id={0} and IsActive=true", EngagementId);
                    OdbcCommand selectCommand = new OdbcCommand(DatasetQuery, connection);
                    OdbcDataReader dsreader = selectCommand.ExecuteReader();
                    DataTable dsDT = new DataTable();
                    dsDT.Load(dsreader);

                    foreach (DataRow dr in dsDT.Rows)
                    {
                        Dataset modelDataset = new Dataset();
                        modelDataset.dataset_name = dr["dataset_name"].ToString();
                        modelDataset.tablename = dr["tablename"].ToString();
                        modelDataset.uploaded_to_databricks = Convert.ToBoolean(dr["uploaded_to_databricks"]);
                        Datasets.Add(modelDataset);
                    }

                    return Datasets;
                }
                catch (Exception ex)
                {
                    _logger.LogError("AddDatasets : " + ex.Message);
                    return Datasets;
                }
            }
        }
        public List<Dataset> RefreshDatasets(int EngagementId)
        {
            string connectionString = GetODBCConnection();
            List<Dataset> Datasets = new List<Dataset>();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try {
                    string DatasetQuery = string.Format("select dd.dataset_name, dd.tablename, dd.uploaded_to_databricks from mna.dataset_details dd where dd.engagement_id={0} and IsActive=true", EngagementId);
                    OdbcCommand selectCommand = new OdbcCommand(DatasetQuery, connection);
                    connection.Open();
                    OdbcDataReader dsreader = selectCommand.ExecuteReader();
                    DataTable dsDT = new DataTable();
                    dsDT.Load(dsreader);

                    foreach (DataRow dr in dsDT.Rows)
                    {
                        Dataset modelDataset = new Dataset();
                        modelDataset.dataset_name = dr["dataset_name"].ToString();
                        modelDataset.tablename = dr["tablename"].ToString();
                        modelDataset.uploaded_to_databricks = Convert.ToBoolean(dr["uploaded_to_databricks"]);
                        Datasets.Add(modelDataset);
                    }

                    return Datasets;
                }
                catch(Exception ex)
                {
                    _logger.LogError("AddDatasets : " + ex.Message);
                    return Datasets;
                }

            }

        }
        public int AddDatasetLog(int EngagementId, string filename, string message, string username)
        {
            string connectionString = GetODBCConnection();
            int records = 0;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    string DatasetActivity_Query = "";
                    DatasetActivity_Query = String.Format("Insert into mna.Dataset_Activity(Engagement_Id," +
                                            "Filename," +
                                            "Message," +
                                            "CreatedUser," +
                                            "CreatedDate," +
                                            "ModifiedUser," +
                                            "ModifiedDate," +
                                            "IsActive" +
                                            ") values({0},'{1}','{2}','{3}',{4},'{5}',{6},{7})", EngagementId, filename, message, username, "getdate()", username, "getdate()", true);
                    connection.Open();
                    OdbcCommand datasetactivityCommand = new OdbcCommand(DatasetActivity_Query, connection);
                    records=datasetactivityCommand.ExecuteNonQuery();
                    datasetactivityCommand.Dispose();
                    return records;

                }
                catch (Exception ex)
                {
                    _logger.LogError("Dataset Activitylog " + ex.Message);
                    return 0;
                }
            }
        }

        public List<DatasetActivity> GetDatasetLog(int EngagementId, string filename)
        {
            string connectionString = GetODBCConnection();
            List<DatasetActivity> datasetactivities = new List<DatasetActivity>();
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try
                {
                    string DatasetActivity_Query = "";
                    DatasetActivity_Query = String.Format("select * from mna.Dataset_Activity where Engagement_Id={0} and Filename='{1}'", EngagementId, filename);
                    connection.Open();
                    OdbcCommand datasetactivityCommand = new OdbcCommand(DatasetActivity_Query, connection);
                    OdbcDataReader dsreader = datasetactivityCommand.ExecuteReader();
                    DataTable dsDT = new DataTable();
                    dsDT.Load(dsreader);

                    foreach (DataRow dr in dsDT.Rows)
                    {
                        DatasetActivity modelDatasetActivity = new DatasetActivity();
                        modelDatasetActivity.Id = Convert.ToInt32(dr["Id"]);
                        modelDatasetActivity.Engagement_Id = Convert.ToInt32(dr["Engagement_Id"]);
                        modelDatasetActivity.Filename = dr["Filename"].ToString();
                        modelDatasetActivity.Message = dr["Message"].ToString();
                        modelDatasetActivity.CreatedUser = dr["CreatedUser"].ToString();
                        modelDatasetActivity.CreatedDate = Convert.ToDateTime(dr["CreatedDate"]);
                        datasetactivities.Add(modelDatasetActivity);
                    }
                    return datasetactivities;

                }
                catch (Exception ex)
                {
                    _logger.LogError("Dataset Activitylog " + ex.Message);
                    return null;
                }
            }
        }
        public string CreateSchema(string schemaname)
        {
            int records = 0;
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "CREATE SCHEMA IF NOT EXISTS " + schemaname;
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    records = command.ExecuteNonQuery();

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError("CreateSchema : " + ex.Message);
                }
            }
            return schemaname;

        }
        public void LogError(string errormessage)
        {
            string connectionString = GetODBCConnection();

            string resp = "";
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = string.Format("insert into mna.errorlog(Error,CreatedDate) values('{0}',{1})", errormessage, "getdate()");
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    command.ExecuteNonQuery();


                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError("LogError to DB : " + ex.Message);
                    resp = ex.Message;
                }
            }

        }

        public void AddJobInfo(int EngagementId, string filename, string jobid, string jobrunid, string number_in_job, string status)
        {
            string connectionString = GetODBCConnection();

            string resp = "";
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = string.Format("insert into mna.jobinfo(EngagementId,Filename,JobId,JobrunId,number_in_job,Status,CreatedDate) values({0},'{1}','{2}','{3}','{4}','{5}',{6})", EngagementId, filename, jobid, jobrunid, number_in_job, status, "getdate()");
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    command.ExecuteNonQuery();


                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError("AddJobInfo : " + ex.Message);
                    resp = ex.Message;
                }
            }

        }

        public string CreateCache(string tablename)
        {
            int records = 0;
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "cache select * from " + tablename;
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    records = command.ExecuteNonQuery();

                    command.Dispose();
                    return "success";
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    return "failure";
                }
            }


        }
        public int UpdateDatasetDetails(int EngagementID, string filename, string Tablename)
        {
            int records = 0;
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = string.Format("update mna.dataset_details set Tablename='{0}',Uploaded_To_Databricks={1},Modifieddate={2} where Engagement_Id={3} and Dataset_Name='{4}'", Tablename, true, "getdate()", EngagementID, filename);
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    records = command.ExecuteNonQuery();

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                }
            }
            return records;

        }

        public string SetSchemaPermissions(string SchemaName, string UserorGroupName)
        {
            string connectionString = GetODBCConnection();

            string resp = "";
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {

                sqlQuery = "GRANT USAGE ON SCHEMA " + SchemaName + " TO " + "`" + UserorGroupName + "`";

                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    command.ExecuteNonQuery();
                    resp = sqlQuery + " success";

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    resp = ex.Message;
                }
            }
            return resp;
        }

        public string SetTablePermissions(string TableName, string UserorGroupName)
        {
            string connectionString = GetODBCConnection();

            string resp = "";
            string sqlQuery = "";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {

                sqlQuery = "GRANT SELECT ON TABLE " + TableName + " TO " + "`" + UserorGroupName + "`";

                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    command.ExecuteNonQuery();
                    resp = sqlQuery + " success";

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    resp = ex.Message;
                }
            }
            return resp;
        }

        public string GetServiceAccount(int engagementid)
        {

            string connectionString = GetODBCConnection();
            string groupname = "";
            string existgroupname = "";

            string resp = "";
            string sqlQuery;
            string existingcheck = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                existingcheck= String.Format("select Accountname from mna.serviceaccountdetails where Engagement_Id = {0} limit 1", engagementid);
                OdbcCommand existCommand=new OdbcCommand(existingcheck, connection);
                connection.Open();
                try
                {

                    OdbcDataReader existreader = existCommand.ExecuteReader();
                    DataTable dtExist = new DataTable();
                    dtExist.Load(existreader);
                    foreach (DataRow dr in dtExist.Rows)
                    {
                        existgroupname = dr["Accountname"].ToString();

                    }

                    if (existgroupname == "") 
                    { 
                    sqlQuery = "select  * from mna.ServiceAccountDetails where Is_inuse=false order by Id limit 1";
                    OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                    OdbcDataReader reader = command.ExecuteReader();
                    DataTable dt = new DataTable();
                    dt.Load(reader);
                    foreach (DataRow dr in dt.Rows)
                    {
                        groupname = dr["Accountname"].ToString();

                    }
                        command.Dispose();
                    }
                    else
                    {
                        groupname = existgroupname;
                    }

                    existCommand.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    resp = ex.Message;
                }
                return groupname;
            }
        }

        public int UpdateServiceAccountDetails(int EngagementID, string Accountname)
        {
            int records = 0;
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "update mna.ServiceAccountDetails set Is_Inuse=true,Engagement_Id=" + EngagementID + " where Accountname='" + Accountname + "'";
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    records = command.ExecuteNonQuery();

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                }
            }
            return records;

        }

        public bool UpdateEngagementClientAccessDetails(ClientAccess data)
        {
            try
            {
                string connectionString = GetODBCConnection();
                string query1;
                string date = "getdate()";
                int rowAffected;
                OdbcCommand? command = null;

                using (OdbcConnection connection = new OdbcConnection(connectionString))
                {
                    connection.Open();
                    query1 = String.Format("select count(*) from mna.client_access where Engagement_Id = '{0}'", data.Engagement_Id);
                    command = new OdbcCommand(query1, connection);

                    int count = 0;
                    count = Convert.ToInt32(command.ExecuteScalar());
                    if (count > 0)
                    {
                        //update here 
                        //update mna.client_access set Is_Paused = !Is_Paused where Engagement_Id = 1
                        query1 = String.Format("Update mna.client_access set Is_Paused={3},modifieduser='{1}',modifieddate={2} where  Engagement_Id ={0}", data.Engagement_Id,data.SubmittedBy,date,data.Is_Paused);
                        command = new OdbcCommand(query1, connection);
                        rowAffected = command.ExecuteNonQuery();
                    }
                    else
                    {
                        //first time control will reach here and make the engagement status as false
                        query1 = String.Format("insert into  mna.client_access" +
                            "(Engagement_Id" +
                            ",Is_Paused" +
                            ",createdUser" +
                            ",createddate" +
                            ",modifieduser" +
                            ",modifieddate" +
                            ",IsActive)values" +
                            "({0},{1},'{2}',{3},'{4}',{5},{6})", data.Engagement_Id, data.Is_Paused, data.SubmittedBy, date, data.SubmittedBy, date, true);
                         command = new OdbcCommand(query1, connection);
                        rowAffected = command.ExecuteNonQuery();

                    }
                    return true;
                }
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public string GetTablename(int engagementId,string filename)
        {

            string connectionString = GetODBCConnection();
            string tablename = "";

            string resp = "";
            string sqlQuery;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = string.Format("select  tablename from mna.dataset_details where engagement_id={0} and dataset_name='{1}' and Isactive=true and uploaded_to_databricks=true",engagementId,filename);
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    OdbcDataReader reader = command.ExecuteReader();
                    DataTable dt = new DataTable();
                    dt.Load(reader);
                    foreach (DataRow dr in dt.Rows)
                    {
                        tablename = dr["tablename"].ToString();

                    }


                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    resp = ex.Message;
                }
                return tablename;
            }
        }

        public string DropTable(string tablename)
        {
            string connectionString = GetODBCConnection();

            string resp = "";
            string sqlQuery = "";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {

                sqlQuery = "DROP TABLE IF EXISTS " + tablename ;

                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    command.ExecuteNonQuery();
                    resp = "success";

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    resp = ex.Message;
                }
            }
            return resp;
        }

        public string DropSchema(string schemaname)
        {
            string connectionString = GetODBCConnection();

            string resp = "";
            string sqlQuery = "";

            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {

                sqlQuery = "DROP SCHEMA IF EXISTS " + schemaname +" CASCADE";

                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    command.ExecuteNonQuery();
                    resp = "success";

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    resp = ex.Message;
                }
            }
            return resp;
        }

        public string GetFoldername(int engagementId)
        {

            string connectionString = GetODBCConnection();
            string foldername = "";

            string resp = "";
            string sqlQuery;
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = string.Format("select folder_name from mna.adls_details where engagement_id={0} and Isactive=true", engagementId);
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    OdbcDataReader reader = command.ExecuteReader();
                    DataTable dt = new DataTable();
                    dt.Load(reader);
                    foreach (DataRow dr in dt.Rows)
                    {
                        foldername = dr["folder_name"].ToString();

                    }


                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    resp = ex.Message;
                }
                return foldername;
            }
        }

        public string UpdateSGGroupDetails(int engagementId)
        {
            string resp = "";
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "update mna.sggroupdetails set Is_Inuse=false,Engagement_Id=null where Engagement_Id=" + engagementId;
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    command.ExecuteNonQuery();
                    resp = "success";
                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError("UpdateSGGroupDetails- Closeout : " + ex.Message);
                    resp = "failure";
                }
            }
            return resp;
        }
        public string UpdateServiceAccountDetails(int EngagementID)
        {
            string resp = "";
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "update mna.ServiceAccountDetails set Is_Inuse=false,Engagement_Id=null where Engagement_Id=" + EngagementID;
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    command.ExecuteNonQuery();
                    resp = "success";

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    resp = "failure";
                }
            }
            return resp;

        }

        public string DeleteTableRecord(string tablename)
        {
            string resp = "";
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                sqlQuery = "update mna.dataset_details set Uploaded_To_Databricks=false where Tablename='" + tablename+"'";
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                try
                {
                    command.ExecuteNonQuery();
                    resp = "success";

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                    resp = "failure";
                }
            }
            return resp;

        }

        public int AddAccessUser(string EmailId,string createduser)
        {
            int count = 0;
            string connectionString = GetODBCConnection();
            string sqlQuery = "";
            using (OdbcConnection connection = new OdbcConnection(connectionString))
            {
                try { 
                sqlQuery = String.Format("select count(*) from mna.users where EmailId = '{0}'", EmailId);
                OdbcCommand command = new OdbcCommand(sqlQuery, connection);
                connection.Open();
                count = Convert.ToInt32(command.ExecuteScalar());

                    if (count == 0)
                    {
                        sqlQuery = String.Format("insert into mna.User_Access("+
                        "EmailID," +
                        "IsActive," +
                        "CreatedUser," +
                        "CreatedDate," +
                        "ModifiedUser," +
                        "ModifiedDate" +
                        ") values('{0}',{1},'{2}',{3},'{4}',{5})", EmailId, true, createduser, "getdate()", createduser, "getdate()");

                        command = new OdbcCommand(sqlQuery, connection);
                        count = command.ExecuteNonQuery();
                    }

                    command.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.Message);
                   
                }
                return count;
            }
            
        }
    }
}
