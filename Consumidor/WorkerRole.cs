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

namespace Consumidor
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        static CloudQueue cloudQueueTwo;

        // Connection to QueueOne and QueueTwo
        public static void ConnectToStorageQueue()
        {
            var connectionString = "DefaultEndpointsProtocol=https;AccountName=stancatti;AccountKey=XlYXd1KWhEw1kj0F5Q5UHPfY+yFJ67Yv6t91iOV5wPtB/lwVBMplLC7dzWVwiAGrqBd6TADwpjEfrFXLy/erig==;EndpointSuffix=core.windows.net";
            CloudStorageAccount cloudStorageAccount;

            if (!CloudStorageAccount.TryParse(connectionString, out cloudStorageAccount))
            {
                Console.WriteLine("Expected connection string 'Azure Storage Account to be a valid Azure Storage Connection String.");
            }

            var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
            cloudQueueTwo = cloudQueueClient.GetQueueReference("queuetwo");

            cloudQueueTwo.CreateIfNotExists();
        }

        //Delete message from QueueOne
        public void DeleteMessageFromQueueTwo()
        {
            CloudQueueMessage cloudQueueMessage = cloudQueueTwo.GetMessage();

            if (cloudQueueMessage == null)
            {
                return;
            }
            Trace.TraceInformation("Deleting message from QueueTwo");
            cloudQueueTwo.DeleteMessage(cloudQueueMessage);
        }


        public override void Run()
        {
            Trace.TraceInformation("Consumidor is running");

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

            Trace.TraceInformation("Consumidor has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("Consumidor is stopping");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("Consumidor has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: substitua o item a seguir pela sua própria lógica.

            ConnectToStorageQueue();

            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Working");
                DeleteMessageFromQueueTwo();
                await Task.Delay(10000);
            }
        }
    }
}
