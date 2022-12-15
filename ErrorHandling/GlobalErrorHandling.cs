using Newtonsoft.Json;
using System.Net;

namespace Deloitte.MnANextGenAnalytics.WebAPI.ErrorHandling
{
    public class GlobalErrorHandling
    {
        private readonly RequestDelegate _next;
        private static ILogger _logger;

        public GlobalErrorHandling(RequestDelegate next, ILogger<GlobalErrorHandling> logger)
        {
            _next = next;
            _logger = logger;
        }

        public async Task Invoke(HttpContext context)
        {
            try
            {
                await _next.Invoke(context);
            }
            catch (Exception ex)
            {
                await HandleExceptionMessageAsync(context, ex).ConfigureAwait(false);
            }
        }
        private static Task HandleExceptionMessageAsync(HttpContext context, Exception exception)
        {
            string errormsg = "Global Internal Server Error";
            context.Response.ContentType = "application/json";
            int statusCode = (int)HttpStatusCode.InternalServerError;
            var result = JsonConvert.SerializeObject(new
            {
                StatusCode = statusCode,
                ErrorMessage = exception.Message
            });
            _logger.LogError(errormsg + exception.Message);
            context.Response.ContentType = "application/json";
            context.Response.StatusCode = statusCode;
            return context.Response.WriteAsync(result);
        }
    }
}
