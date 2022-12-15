using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.IdentityModel.Tokens.Jwt;
using System.Net;
using Deloitte.MnANextGenAnalytics.WebAPI.DataLayer;


[AttributeUsage(AttributeTargets.Class)]
public class CustomAuthorization : Attribute, IAuthorizationFilter
{
    private  IConfiguration _config;
    public DataLayer? datalayer;
    private ILogger _logger;

    
    /// <summary>  
    /// This will Authorize User  
    /// </summary>  
    /// <returns></returns>  
    public void OnAuthorization(AuthorizationFilterContext filterContext)
    {
        
        if (filterContext != null)
        {
            _config=filterContext.HttpContext.RequestServices.GetService<IConfiguration>();
            _logger= filterContext.HttpContext.RequestServices.GetService<ILogger<CustomAuthorization>>();
            Microsoft.Extensions.Primitives.StringValues authTokens;
            filterContext.HttpContext.Request.Headers.TryGetValue("Authorization", out authTokens);

            var _token = authTokens.FirstOrDefault();


            if (_token != null)
            {
                string authToken = _token;
                if (authToken != null)
                {
                    if (IsValidToken(authToken))
                    {
                        filterContext.HttpContext.Response.Headers.Add("authToken", authToken);
                        filterContext.HttpContext.Response.Headers.Add("AuthStatus", "Authorized");

                        filterContext.HttpContext.Response.Headers.Add("storeAccessiblity", "Authorized");

                        return;
                    }
                    else
                    {
                        filterContext.HttpContext.Response.Headers.Add("authToken", authToken);
                        filterContext.HttpContext.Response.Headers.Add("AuthStatus", "NotAuthorized");

                        filterContext.HttpContext.Response.StatusCode = (int)HttpStatusCode.Forbidden;
                       filterContext.Result = new JsonResult("NotAuthorized")
                        {
                            Value = new
                            {
                                Status = "Error",
                                Message = "Invalid Token"
                            },
                        };
                    }

                }

            }
            else
            {
                filterContext.HttpContext.Response.StatusCode = (int)HttpStatusCode.ExpectationFailed;
                filterContext.Result = new JsonResult("Please Provide authToken")
                {
                    Value = new
                    {
                        Status = "Error",
                        Message = "Please Provide authToken"
                    },
                };
            }
        }
    }

    public bool IsValidToken(string authToken)
    {
        var handler = new JwtSecurityTokenHandler();
        var jsonToken = handler.ReadToken(authToken.Replace("Bearer ", ""));
        var tokenS = jsonToken as JwtSecurityToken;
        var emailId = tokenS.Payload.Where(obj => obj.Key == "unique_name").Select(obj => obj.Value).FirstOrDefault();        
        if (emailId != null)
        {
            datalayer = new DataLayer(_config, _logger);
            var result = datalayer.ValidateUser(emailId.ToString());
            return result;
        }
        else
        {
            return false;
        }
    }
}