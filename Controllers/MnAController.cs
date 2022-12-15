using Azure;
using Azure.Core;
using Azure.Storage.Blobs;
using Deloitte.MnANextGenAnalytics.WebAPI.BusinessLayer;
using Deloitte.MnANextGenAnalytics.WebAPI.DataModels;
using Deloitte.MnANextGenAnalytics.WebAPI.Mails;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.Extensions.FileSystemGlobbing.Internal;
using Microsoft.IdentityModel.Tokens;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Mime;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;

namespace Deloitte.MnANextGenAnalytics.WebAPI.Controllers
{
    [CustomAuthorization]
    [Route("api/[controller]")]
    [ApiController]
    public class MnAController : ControllerBase
    {
        private static IConfiguration _config;
        public DataLayer.DataLayer dataLayer;
        public ADLSLayer aDLSLayer;
        private static ILogger _logger;
        
        public MnAController(IConfiguration config, ILogger<MnAController> logger)
        {
            _config = config;
            _logger = logger;
            dataLayer = new DataLayer.DataLayer(config, logger);
            aDLSLayer=new ADLSLayer(config, logger);
        }
        
        [HttpPost]
        public ActionResult SaveEngagementDetails(EngagementDetails data)
        {
            try
            {
                dataLayer = new DataLayer.DataLayer(_config, _logger);
                var validWBS = ValidateWBSCodes(data.wbscode, data.wbsAccesToken).Result;
                int result;
                string folderurl = "";
                bool dt;
                EngagementData oldValue = new EngagementData();
                //

                if ((data.id != null) && (data.id > 0))
                {
                    oldValue = dataLayer.GetEngagementData(data.id ?? 0);
                    
                    if (!validWBS)
                    {
                        data.wbscode = "MNA12345-01-01-01-0001";
                    }
                    result = dataLayer.UpdateEngagementDetails(data);
                    if (result != -1)
                    {
                       
                        SendEmail(data);
                        if (data.currentStatus >= 2)
                        {
                            dt = dataLayer.UpdateActivityLog(oldValue, data);
                        }
                    }
                    return CreatedAtAction(nameof(SaveEngagementDetails), result);
                }
                else
                {
                    if (!validWBS)
                    {
                        data.wbscode = "MNA12345-01-01-01-0001";
                    }
                    result = dataLayer.SaveEngagementDetails(data);
                    data.id = result;
                    if (result != -1)
                        SendEmail(data);
                    return CreatedAtAction(nameof(SaveEngagementDetails), result);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("SaveEngagementDetails : " + ex.Message);
                //dataLayer.LogError("SaveEngagementDetails : " + ex.Message);
                string errorjson = "{" + "\"Name\":\"Error\",\"value\":\"" + ex.Message + "\" }";
                return CreatedAtAction(nameof(SaveEngagementDetails), errorjson);
            }

        }
        [HttpPost("InitialsubmitApprove")]
        public ActionResult InitialsubmitApprove(EngagementDetails data)
        {
            try
            {
                dataLayer = new DataLayer.DataLayer(_config, _logger);
                int result;
                string folderurl = "";
                string json = "";
                EngagementData oldValue = new EngagementData();
                if ((data.id != null) && (data.id > 0))     
                {
                    oldValue = dataLayer.GetEngagementData(data.id ?? 0);
                    var validWBS = ValidateWBSCodes(data.wbscode, data.wbsAccesToken).Result;
                    if (!validWBS)
                    {
                        data.wbscode = "MNA12345-01-01-01-0001";
                    }
                     result = dataLayer.UpdateEngagementDetails(data);
                    if (result != -1)
                    {
                        //add logic here to insert the data into activity log
                        if (data.currentStatus >= 2)
                        {
                            bool dt = dataLayer.UpdateActivityLog(oldValue, data);
                        }
                        //
                        SendEmail(data);
                        folderurl = aDLSLayer.CreateFolder(data.engagementname, data.wbscode, data.id);
                        json = "{" + "\"Name\":\"" + data.engagementname + "-" + data.wbscode + "\",\"value\":\"" + folderurl + "\" }";
                    }

                }
                return CreatedAtAction(nameof(InitialsubmitApprove), json);
            }
            catch (Exception ex)
            {
                _logger.LogError("InitialsubmitApprove : " + ex.Message);
                //dataLayer.LogError("InitialsubmitApprove : " + ex.Message);
                string errorjson = "{" + "\"Name\":\"Error\",\"value\":\"" + ex.Message + "\" }";
                return CreatedAtAction(nameof(InitialsubmitApprove), errorjson);
            }

        }

        // GET: api/<MnAController>
        [HttpGet]
        public IEnumerable<EngagementPortfolio> Get()
        {
            try
            {
                dataLayer = new DataLayer.DataLayer(_config, _logger);
                List<EngagementPortfolio> Portfolio = dataLayer.GetOfferingPortfolio();
                return Portfolio;
            }
            catch (Exception ex)
            {
                _logger.LogError("GetPortfolio : " + ex.Message);
                //dataLayer.LogError("GetPortfolio : " + ex.Message);
                return null;
            }
            //return new string[] { "HealthCare", "MedicalCare" };
        }       

        [HttpGet("GetSecurityMatrix")]
        public async Task<List<SecurityMatrix>> GetSecurityMatrix()
        {
            try
            {
                dataLayer = new DataLayer.DataLayer(_config, _logger);
                var data = dataLayer.GetSecurityMatrix();
                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError("GetSecurityMatrix : " + ex.Message);
                //dataLayer.LogError("GetSecurityMatrix : " + ex.Message);
                return null;
            }
        }

        [HttpGet("CheckEngagementAccess")]
        public async Task<bool> EngagementAccessibleToUser(int EngagementId, string EmailId)
        {
            try
            {
                if (EngagementId == 0)
                    return true;
                else
                {
                    dataLayer = new DataLayer.DataLayer(_config, _logger);
                    var data = dataLayer.CheckEngagementAccess(EngagementId, EmailId);
                    return data;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("CheckEngagementAccess : " + ex.Message);
                //dataLayer.LogError("CheckEngagementAccess : " + ex.Message);
                return false;
            }
        }
            

            [HttpGet("GetDatabricksLoginInfo")]
        public async Task<DatabricksLoginInfo> GetDatabricksLoginInfo(int EngagementId)
        {
            try
            {
                dataLayer = new DataLayer.DataLayer(_config, _logger);
                var data = dataLayer.GetDatabricksLoginInfo(EngagementId);
                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError("GetDatabricksLoginInfo : " + ex.Message);
                //dataLayer.LogError("GetDatabricksLoginInfo : " + ex.Message);
                return null;
            }
        }
        [HttpGet("GetEngagementDetails")]
        public async Task<EngagementData> GetEngagementData(int EngagementId)
        {
            try
            {
                dataLayer = new DataLayer.DataLayer(_config, _logger);
                EngagementData data = dataLayer.GetEngagementData(EngagementId);
                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError("GetEngagementDetails : " + ex.Message);
                //dataLayer.LogError("GetEngagementDetails : " + ex.Message);
                return null;
            }
        }

        
        [HttpGet("GetAllEngagements")]
        public async Task<List<DashboardData>> GetAllEngagements(string EmailId)
        {
            try
            {
                dataLayer = new DataLayer.DataLayer(_config, _logger);
                List<DashboardData> data = dataLayer.GetAllEngagements(EmailId);
                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError("GetAllEngagements : " + ex.Message);
                //dataLayer.LogError("GetAllEngagements : " + ex.Message);
                return null;
            }
        }

      
        
        private static string GetKeyVault(string ServiceAccount)
        {
            try
            {
                string keyVaultURL = _config.GetValue<string>("AzureDetails:AzureKeyVaultURL");
                KeyVaultClient kvc = null;
                kvc = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(GetToken));

                SecretBundle secret = Task.Run(() => kvc.GetSecretAsync(keyVaultURL +
                    @"/secrets/" + ServiceAccount)).ConfigureAwait(false).GetAwaiter().GetResult();
                return secret.Value;
            }
            catch (Exception ex)
            {
                _logger.LogError("GetKeyVault : " + ex.Message);
                throw ex;
            }
        }
        private static async Task<string> GetToken(string authority, string resource, string scope)
        {
            try
            {
                string CLIENTID = _config.GetValue<string>("AzureDetails:ClientId");
                string CLIENTSECRET = _config.GetValue<string>("AzureDetails:ClientSecret");
                var authContext = new Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationContext(authority);
                Microsoft.IdentityModel.Clients.ActiveDirectory.ClientCredential clientCred = new Microsoft.IdentityModel.Clients.ActiveDirectory.ClientCredential(CLIENTID, CLIENTSECRET);
                Microsoft.IdentityModel.Clients.ActiveDirectory.AuthenticationResult result = await authContext.AcquireTokenAsync(resource, clientCred);
                
                if (result == null)
                    throw new InvalidOperationException("Failed to obtain the JWT token");

                return result.AccessToken;
            }
            catch (Exception ex)
            {
                _logger.LogError("GetToken : " + ex.Message);
                return null;
            }
        }
        // POST api/<MnAController>
        //[HttpPost]
        //public void Post([FromBody] string value)
        //{

        //}

        // PUT api/<MnAController>/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE api/<MnAController>/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }

        // POST api/<SendEmail>
        [HttpPost("SendEmail")]
        public string SendEmail(EngagementDetails engagementDetails)
        
        {
            try
            {
                SMTPMail sMTPMail = new SMTPMail();
                sMTPMail.Send(engagementDetails);
                return "Success";
            }
            catch (Exception ex)
            {
                _logger.LogError("SendEMail : " + ex.Message);
                //dataLayer.LogError("SendEMail : " + ex.Message);
                return "failure";
            }
        }

        [HttpGet("GetFiles")]
        public List<Dataset> GetFiles(int EngagementId, string username,string foldername)
        {
            try
            {
                string Foldername = foldername;
                string ConnectionString = _config.GetValue<string>("Blobstorage:ConnectionString");
                string containername = _config.GetValue<string>("Blobstorage:containername");
                BlobServiceClient blobServiceClient = new BlobServiceClient(ConnectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containername);
                var blobs = containerClient.GetBlobs();
                List<string> names = blobs.Where(x => x.Name.Contains(Foldername + "/")&&x.Name.ToLower().EndsWith(".csv")).Select(x => x.Name.Replace(Foldername + "/", "")).ToList();
                List<Dataset> files=dataLayer.AddDatasets(names, EngagementId, username);
                return files;
            }
            catch (Exception ex)
            {
                _logger.LogError("GetFiles : " + ex.Message);
                return null;
            }
        }

        [HttpGet("RefreshDataset")]
        public List<Dataset> RefreshDataset(int EngagementId)
        {
            try
            {
                List<Dataset> files = dataLayer.RefreshDatasets(EngagementId);
                return files;
            }
            catch(Exception ex)
            {
                _logger.LogError("GetFiles : " + ex.Message);
                return null;
            }
        }


        [HttpGet("GenerateWBSToken")]
        public async Task<WBSTokenResponse> GenerateWBSToken()
        {
            try
            {
                var client = new HttpClient();
                var clientId = _config.GetValue<string>("WBSApiDetails:NProdClientId");
                var clientSecret = _config.GetValue<string>("WBSApiDetails:NProdClientSecret");
                var url = _config.GetValue<string>("WBSApiDetails:TokenURL");
                var resource = _config.GetValue<string>("WBSApiDetails:NProdResource");


                List<KeyValuePair<string, string>> request = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>("client_id", clientId),
                new KeyValuePair<string, string>("grant_type", "client_credentials"),
                new KeyValuePair<string, string>("client_secret", clientSecret),
                new KeyValuePair<string, string>("resource", resource),
             };
                var content = new FormUrlEncodedContent(request);
                var response = await client.PostAsync(url, content);
                var result = await response.Content.ReadAsStringAsync();
                WBSTokenResponse tokenResponse = JsonConvert.DeserializeObject<WBSTokenResponse>(result);
                return tokenResponse;
            }
            catch (Exception ex)
            {
                _logger.LogError("Token : " + ex.Message);
                //dataLayer.LogError("Token : " + ex.Message);
                return null;
            }

        }

        [HttpPost("UpdateClientAccess")]
        public ActionResult UpdateEngagementClientAccessDetails(ClientAccess data)
        {
            try
            {
                dataLayer = new DataLayer.DataLayer(_config, _logger);

                bool result=true;                

                if ( data.Engagement_Id > 0)
                {
                    result = dataLayer.UpdateEngagementClientAccessDetails(data);
                  //   if (result != false)
                 //        SendEmail(data);
                }
                return CreatedAtAction(nameof(UpdateEngagementClientAccessDetails), result);


            }
            catch (Exception ex)
            {
                _logger.LogError("UpdateEngagementClientAccessDetails : " + ex.Message);
                //dataLayer.LogError("UpdateEngagementClientAccessDetails : " + ex.Message);
                string errorjson = "{" + "\"Name\":\"Error\",\"value\":\"" + ex.Message + "\" }";
                return CreatedAtAction(nameof(UpdateEngagementClientAccessDetails), errorjson);
            }

        }


        [HttpPost("GetWBSCodes")]
        public async Task<List<WBSSearchResponse>> GetWBSCodes(WBSSearchRequest wBSTokenRequest)
        {
            var ApiUrl = _config.GetValue<string>("WBSApiDetails:NProdApiUrl");
            List<WBSSearchResponse> filteredWBS = new List<WBSSearchResponse>();
            var regexPattern = "[a-zA-Z0-9]{8}\\-?[0-9]{2}\\-?[0-9]{2}\\-?[0-9]{2}\\-?[a-zA-Z0-9]{4}";
            Regex rg = new Regex(regexPattern);
            string apiResponse = string.Empty;
            try
            {
                string body = "{" +
                                 "\"searchText\": \"" + wBSTokenRequest.searchText + "\"," +
                                 "\"page\": \"" + wBSTokenRequest.page + "\"," +
                                 "\"pageSize\": \"" + wBSTokenRequest.pageSize + "\"," +
                                 "\"order\": [" +
                                "{" +
                                "\"orderBy\": \"" + wBSTokenRequest.order[0].orderBy + "\"," +
                                "\"orderDir\": \"" + wBSTokenRequest.order[0].orderDir + "\"" +
                                 "}," +
                                 "{" +
                                 "\"orderBy\": \"" + wBSTokenRequest.order[1].orderBy + "\"," +
                                "\"orderDir\": \"" + wBSTokenRequest.order[1].orderDir + "\"" +
                                 "}" +
                                "]," +
                               "}";
                using (var httpClient = new HttpClient())
                {
                    httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + wBSTokenRequest.accssToken);
                    var request = new HttpRequestMessage
                    {
                        Method = HttpMethod.Post,
                        RequestUri = new Uri(ApiUrl),
                        Content = new StringContent(body, Encoding.UTF8, MediaTypeNames.Application.Json),

                    };
                    using (var response = await httpClient.SendAsync(request))
                    {
                        
                        apiResponse = await response.Content.ReadAsStringAsync();
                       var wBSSearchResponse = JsonConvert.DeserializeObject <List<WBSSearchResponse>>(apiResponse);
                        
                        foreach (var wbs in wBSSearchResponse)
                        {
                            var wbstring = rg.Matches(wbs.WBS);
                            if (wbstring.Count > 0)
                            {
                                filteredWBS.Add(wbs);
                            }                            
                        }
                    }
                }

                return filteredWBS;
            }
            catch (Exception ex)
            {
                _logger.LogError("GraphAPI Service : " + ex.Message);
                //dataLayer.LogError("GraphAPI Service : " + ex.Message);
                throw new Exception("Graph api service call failed. \n" + ex.Message);
            }

            // return URI of the created resource.



        }        
        private async Task<bool> ValidateWBSCodes(string searchText,string accssToken)
        {
            var ApiUrl = _config.GetValue<string>("WBSApiDetails:NProdApiUrl");                  
            string apiResponse = string.Empty;
            var regexPattern = "[a-zA-Z0-9]{8}\\-?[0-9]{2}\\-?[0-9]{2}\\-?[0-9]{2}\\-?[a-zA-Z0-9]{4}";
            Regex rg = new Regex(regexPattern);
            try
            {
                var wbstring = rg.Matches(searchText);
                if (wbstring.Count == 0|| searchText== "MNA12345-01-01-01-0001")
                {
                    return false;
                }
                else
                {
                    string body = "{" +
                                "\"searchText\": \"" + searchText + "\"" +
                              "}";
                    using (var httpClient = new HttpClient())
                    {
                        httpClient.DefaultRequestHeaders.Add("Authorization", "Bearer " + accssToken);
                        var request = new HttpRequestMessage
                        {
                            Method = HttpMethod.Post,
                            RequestUri = new Uri(ApiUrl),
                            Content = new StringContent(body, Encoding.UTF8, MediaTypeNames.Application.Json),

                        };
                        using (var response = await httpClient.SendAsync(request))
                        {

                            apiResponse = await response.Content.ReadAsStringAsync();
                            var wBSSearchResponse = JsonConvert.DeserializeObject<List<WBSSearchResponse>>(apiResponse);
                            foreach (var wbs in wBSSearchResponse)
                            {
                                var isValid = rg.Matches(wbs.WBS);
                                if (isValid.Count > 0)
                                {
                                    return true;
                                }
                            }

                        }
                    }                   
                }

                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError("WBS code validation : " + ex.Message);
                //dataLayer.LogError("GraphAPI Service : " + ex.Message);
                throw new Exception("WBS code validation call failed. \n" + ex.Message);
            }
        }
    }
}
