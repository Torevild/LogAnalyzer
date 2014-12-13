using System;
using System.Collections.Generic;
using System.Net;
using Nest;

namespace LogAnalyzer
{
    public class ElasticSearchProxy
    {
        private ElasticClient _Client;
        private readonly Uri _ConnectionUri;
        private bool _IsConnected;


        public ElasticSearchProxy(Uri connectionUri)
        {
            if (connectionUri != null)
            {
                _ConnectionUri = connectionUri;
            }
            else
            {
                _ConnectionUri = new Uri("http://localhost:9200");
            }
        }

        public ElasticClient GetConnection()
        {
            if (!_IsConnected)
            {
                Connect();
                _IsConnected = true;
            }

            if (!IsConnected())
            {
                _IsConnected = false;
                throw new Exception(string.Format("Cannot connect to elasticsearch process at {0}:{1}. {2}", _ConnectionUri.Host, _ConnectionUri.Port));
            }

            return _Client;
        }

        private void Connect()
        {
            var settings = new ConnectionSettings(
                 _ConnectionUri,
                "my-application"
                 );

            _Client = new ElasticClient(settings);
        }

        private bool IsConnected()
        {
            try
            {
                ICatResponse<CatHealthRecord> catHealthResponse = _Client.CatHealth(x => x.Local());
                if (catHealthResponse != null && catHealthResponse.ConnectionStatus.Success)
                {
                    return true;
                }

                if (catHealthResponse != null)
                {
                    Console.WriteLine("Connection status is {0}", catHealthResponse.ConnectionStatus.Success);
                }
                else
                {
                    Console.WriteLine("Connection response is null");
                }
            }
            catch (WebException ex)
            {
                Console.WriteLine("Got exception:{0} when attempt to connect to elasticsearch process at {0}:{1}. {2}", ex.Message, _ConnectionUri.Host, _ConnectionUri.Port);
            }
            return false;
        }
    }
}
