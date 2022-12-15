using Deloitte.MnANextGenAnalytics.WebAPI.DataModels;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System.Net.Http.Headers;

namespace Deloitte.MnANextGenAnalytics.WebAPI.Controllers
{

    [Route("api/[controller]")]
    [ApiController]
    public class ProfileController : ControllerBase
    {
        private static IConfiguration _config;
        public DataLayer.DataLayer dataLayer;
        private static ILogger _logger;
        public ProfileController(IConfiguration config,ILogger<ProfileController> logger)
        {
            _config = config;
            _logger = logger;
        }

        // GET api/<MnAController>/5
        [AllowAnonymous]
        [HttpGet("{criteria}")]
        public async Task<string> Get(string criteria)
        {
            try
            {
                string result = string.Empty;
                var token = "";
                token = await Token();
                result = (string)await GetProfile(criteria, token);
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError("Criteria : " + ex.Message);
                //dataLayer.LogError("Criteria : " + ex.Message);
                return string.Empty;
            }
        }

        private async Task<Object> GetProfile(string criteria, string token)
        {
            string result = string.Empty;
            var graphapi = "";
            var graphApiUrl = _config.GetValue<string>("AzureDetails:GraphApiUrl");
            HttpResponseMessage httpResponseMessage;

            try
            {
                using (HttpClient client = new HttpClient())
                {

                    if (!criteria.Contains("@"))
                    {
                        graphapi = String.Concat(graphApiUrl, "v1.0/users?$search= \"displayName:{0}\" OR \"mail:{0}\" &$filter=endsWith(mail,'@deloitte.com') &$orderby = displayName &$count = true &$top=5");
                        //graphapi = String.Concat(graphApiUrl, "v1.0/users?$search= \"displayName:{0}\" OR \"mail:{0}\"  &$orderby = displayName &$count = true &$top=5");
                    }
                    else
                    {
                        graphapi = String.Concat(graphApiUrl, "v1.0/users?$filter=(mail eq '{0}' or startswith(mail,'{0}') )and endsWith(mail,'@deloitte.com')&$count=true &$top=5");
                        //graphapi = String.Concat(graphApiUrl, "v1.0/users?$filter=(mail eq '{0}' or startswith(mail,'{0}') ) &$count=true &$top=5");
                    }
                    string url = string.Format(graphapi, criteria);
                    HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, url);
                    request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
                    request.Headers.Add("ConsistencyLevel", "eventual");
                    httpResponseMessage = await client.SendAsync(request).ConfigureAwait(false);
                }
                if (httpResponseMessage != null)
                {
                    if (httpResponseMessage.IsSuccessStatusCode)
                    {
                        result = await GetProfileURLs(httpResponseMessage, token).ConfigureAwait(false);
                    }
                }
                else
                {
                    throw new Exception("Graph api service call failed. \n" + httpResponseMessage.ReasonPhrase);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("GraphAPI Service : " + ex.Message);
                //dataLayer.LogError("GraphAPI Service : " + ex.Message);
                throw new Exception("Graph api service call failed. \n" + ex.Message);
            }
            return result;
        }

        private static async Task<string> GetProfileURLs(HttpResponseMessage httpResponse, string token)
        {
            string result = string.Empty;
            try
            {
                result = httpResponse.Content.ReadAsStringAsync().Result;
                ProfileResponse profileResponse = JsonConvert.DeserializeObject<ProfileResponse>(result);
                if (profileResponse.value.Count > 0)
                {
                    List<ProfileBatchRequest> profileBatchRequest = new List<ProfileBatchRequest>();
                    int counter = 1;
                    for (int i = 0; i < profileResponse.value.Count; i++)
                    {
                        ProfileBatchRequest profile = new ProfileBatchRequest();
                        profile.id = Convert.ToString(counter++);
                        profile.method = "GET";
                        profile.url = string.Format("/users/{0}/photos/48x48/$value", profileResponse.value[i].mail);
                        profileBatchRequest.Add(profile);
                    }
                    result = await BindProfileURLs(profileBatchRequest, token, profileResponse).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Graph api service call failed. \n" + httpResponse.ReasonPhrase);
                throw new Exception("Graph api service call failed. \n" + httpResponse.ReasonPhrase);
            }
            return result;
        }
        private static async Task<string> BindProfileURLs(List<ProfileBatchRequest> profileBatchRequest, string token, ProfileResponse profileResponse)
        {
            string result = string.Empty;
            var graphProfileApiUrl = _config.GetValue<string>("AzureDetails:GraphProfileApiUrl");
            try
            {
                using (HttpClient client = new HttpClient())
                {
                    client.DefaultRequestHeaders.Add("Authorization", "Bearer " + token);
                    var strrequest = JsonConvert.SerializeObject(profileBatchRequest);
                    var jsonRequest = "{'requests':" + strrequest + " }";
                    StringContent content = new StringContent(jsonRequest, System.Text.Encoding.UTF8, "application/json");
                    using (var response = await client.PostAsync(graphProfileApiUrl, content).ConfigureAwait(false))
                    {
                        var graphProfileResult = response.Content.ReadAsStringAsync().Result;
                        ProfileBatchResponse profileBatchResponse = JsonConvert.DeserializeObject<ProfileBatchResponse>(graphProfileResult);
                        var sortedResponse = profileBatchResponse?.responses?.OrderBy(x => x.id).ToArray();
                        for (int i = 0; i < profileBatchResponse.responses.Count; i++)
                        {
                            profileResponse.value[i].profilepicture = sortedResponse[i].body;
                        }
                        result = JsonConvert.SerializeObject(profileResponse);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("BindProfileURLs : " + ex.Message);
                throw new Exception("Graph api service call failed. \n" + ex.Message);
            }
            return result;
        }
        private async Task<string> Token()
        {
            try
            {
                var client = new HttpClient();
                var clientId = _config.GetValue<string>("AzureDetails:ClientId");
                var clientSecret = _config.GetValue<string>("AzureDetails:ClientSecret");
                var username = _config.GetValue<string>("AzureDetails:ServiceAccountUsername");
                var graphApiUrl = _config.GetValue<string>("AzureDetails:GraphApiUrl");
                var aadInstance = _config.GetValue<string>("AzureDetails:ida:AADInstanc");
                var Url = String.Concat(aadInstance, "/oauth2/v2.0/token");
                if (!username.Contains("@")) { username = username + _config.GetValue<string>("AzureDetails:DeloitteEmailDomain"); }
                var password = _config.GetValue<string>("AzureDetails:ServiceAccount_Password");
                List<KeyValuePair<string, string>> request = new List<KeyValuePair<string, string>>
            {
                new KeyValuePair<string, string>("client_id", clientId),
                new KeyValuePair<string, string>("grant_type", "password"),
                new KeyValuePair<string, string>("username",username ),
                new KeyValuePair<string, string>("password",password ),
                new KeyValuePair<string, string>("client_secret", clientSecret),
                new KeyValuePair<string, string>("scope", String.Concat(graphApiUrl,".default"))
            };
                var content = new FormUrlEncodedContent(request);
                var response = await client.PostAsync(Url, content);
                var result = await response.Content.ReadAsStringAsync();
                TokenResponse tokenResponse = JsonConvert.DeserializeObject<TokenResponse>(result);
                return tokenResponse.access_token;
            }
            catch (Exception ex)
            {
                _logger.LogError("Token : " + ex.Message);
                //dataLayer.LogError("Token : " + ex.Message);
                return string.Empty;
            }
        }

    }
}
