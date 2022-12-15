FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443
EXPOSE 25

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["Deloitte.MnANextGenAnalytics.WebAPI.csproj", "."]
RUN dotnet restore "./Deloitte.MnANextGenAnalytics.WebAPI.csproj"
COPY . .
WORKDIR "/src/."
RUN dotnet build "Deloitte.MnANextGenAnalytics.WebAPI.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Deloitte.MnANextGenAnalytics.WebAPI.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install sendmail -y
RUN apt-get install -y odbcinst1debian2 libodbc1 odbcinst unixodbc
RUN apt-get install -y libsasl2-modules-gssapi-mit
RUN apt-get install wget -y
RUN apt-get install unzip -y
RUN apt-get -y install dpkg
RUN apt-get install apt-utils
#RUN apt-get install gdebi -y
RUN wget https://databricks-bi-artifacts.s3.us-east-2.amazonaws.com/simbaspark-drivers/odbc/2.6.26/SimbaSparkODBC-2.6.26.1045-Debian-64bit.zip
RUN unzip SimbaSparkODBC-2.6.26.1045-Debian-64bit.zip
RUN dpkg -i *.deb
RUN find / -type f -name *.ini
RUN export ODBCINI=/etc/odbc.ini ODBCSYSINI=/etc/odbcinst.ini SIMBASPARKINI=/opt/simba/spark/lib/64/simba.sparkodbc.ini
RUN apt-get install -y gawk

RUN gawk -i inplace '{ print } ENDFILE { print "[ODBC Drivers]" }' /opt/simba/spark/Setup/odbcinst.ini
RUN gawk -i inplace '{ print } ENDFILE { print "Simba Apache Spark ODBC Connector 64-bit=Installed" }' /opt/simba/spark/Setup/odbcinst.ini
RUN gawk -i inplace '{ print } ENDFILE { print "[Simba Apache Spark ODBC Connector 64-bit]" }' /opt/simba/spark/Setup/odbcinst.ini
RUN gawk -i inplace '{ print } ENDFILE { print "Description=Simba Apache Spark ODBC Connector (64-bit)" }' /opt/simba/spark/Setup/odbcinst.ini
RUN gawk -i inplace '{ print } ENDFILE { print "Driver=/opt/simba/spark/lib/64/libsparkodbc_sb64.so" }' /opt/simba/spark/Setup/odbcinst.ini

RUN gawk -i inplace '{ print } ENDFILE { print "[ODBC Data Sources]" }'  /etc/odbc.ini
RUN gawk -i inplace '{ print } ENDFILE { print "Simba Spark=Simba Apache Spark ODBC Connector 64-bit" }'  /etc/odbc.ini
RUN gawk -i inplace '{ print } ENDFILE { print "[Simba Spark]" }'  /etc/odbc.ini
RUN gawk -i inplace '{ print } ENDFILE { print "Driver=/opt/simba/spark/lib/64/libsparkodbc_sb64.so" }'  /etc/odbc.ini
RUN gawk -i inplace '{ print } ENDFILE { print "SparkServerType=3" }'  /etc/odbc.ini
RUN gawk -i inplace '{ print } ENDFILE { print "ServiceDiscoveryMode=No Service Discovery" }'  /etc/odbc.ini
RUN gawk -i inplace '{ print } ENDFILE { print "Host=https://adb-8950992905383420.0.azuredatabricks.net" }'  /etc/odbc.ini
RUN gawk -i inplace '{ print } ENDFILE { print "Port=443" }'  /etc/odbc.ini
RUN gawk -i inplace '{ print } ENDFILE { print "UID=token" }'  /etc/odbc.ini
RUN gawk -i inplace '{ print } ENDFILE { print "PWD=dapiddf42b0f839f104a60297578f8bb5419-2" }'  /etc/odbc.ini
ENTRYPOINT ["dotnet", "Deloitte.MnANextGenAnalytics.WebAPI.dll"]
