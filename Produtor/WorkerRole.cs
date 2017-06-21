using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Produtor
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        static CloudQueue cloudQueueOne;
        static CloudQueue cloudQueueTwo;

        // Connection to QueueOne and QueueTwo
        public static void ConnectToStorageQueue()
        {
            var connectionString = "DefaultEndpointsProtocol=https;AccountName=storagenumvem;AccountKey=srFtVmHz9bTufxWtPsTZ8cfzX1avvaW77fo9M7pq82mUCnEFpuHDQqt/LrN6u0pkhJs4RxHigZdCpWjZPZoanQ==;EndpointSuffix=core.windows.net";
            CloudStorageAccount cloudStorageAccount;

            if (!CloudStorageAccount.TryParse(connectionString, out cloudStorageAccount))
            {
                Console.WriteLine("Expected connection string 'Azure Storage Account to be a valid Azure Storage Connection String.");
            }

            var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
            cloudQueueOne = cloudQueueClient.GetQueueReference("queueone");
            cloudQueueTwo = cloudQueueClient.GetQueueReference("queuetwo");

            cloudQueueOne.CreateIfNotExists();
            cloudQueueTwo.CreateIfNotExists();
        }

        //Send message to QueueTwo
        public void SendMessageToQueueTwo(String MessageText)
        {
            var message = new CloudQueueMessage(MessageText);

            cloudQueueTwo.AddMessage(message);

        }

        //Get message from QueueOne
        public void GetMessageFromQueueOne()
        {
            CloudQueueMessage cloudQueueMessage = cloudQueueOne.GetMessage();

            if (cloudQueueMessage == null)
            {
                return;
            }
            Trace.TraceInformation("Get message from QueueOne and send to QueueTwo");
            SendMessageToQueueTwo(cloudQueueMessage.AsString);
            cloudQueueOne.DeleteMessage(cloudQueueMessage);
        }


        public override void Run()
        {
            Trace.TraceInformation("Produtor is running");

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            // Definir o número máximo de conexões simultâneas
            ServicePointManager.DefaultConnectionLimit = 12;

            // Para obter informações sobre como tratar as alterações de configuração
            // veja o tópico do MSDN em https://go.microsoft.com/fwlink/?LinkId=166357.

            bool result = base.OnStart();

            Trace.TraceInformation("Produtor has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("Produtor is stopping");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("Produtor has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: substitua o item a seguir pela sua própria lógica.

            ConnectToStorageQueue();

            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Working");
                GetMessageFromQueueOne();
                await Task.Delay(5000);
            }
        }
    }
}
