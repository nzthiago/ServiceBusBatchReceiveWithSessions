using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;

namespace SBMessageReceiverTest
{
    class Program
    {
        const string sbQueueName = "<your SB queue with Sessions enabled>";
        const string sbConnString = "<Your SB conn string>";
        static IMessageSender messageSender;
        static ISessionClient messageSessionClient;
        
        static void Main(string[] args)
        {
            MainAsync().GetAwaiter().GetResult();
        }
        static async Task MainAsync()
        {

            const int numberOfMessages = 10;
            const int numberOfSessions = 2;
            const int numberOfMessagesToReceiveInBatch = 5;
            messageSender = new MessageSender(sbConnString, sbQueueName);
            messageSessionClient = new SessionClient(sbConnString, sbQueueName);

            
            // Send Messages
            await SendMessagesWithSessionAsync(numberOfMessages, numberOfSessions);

            // Receive Messages
            await ReceiveMessagesWithSessionAsync(numberOfMessagesToReceiveInBatch, "1");
            
            Console.WriteLine("=========================================================");
            Console.WriteLine("Completed Receiving all messages... Press any key to exit");
            Console.WriteLine("=========================================================");
            Console.ReadKey();

            await messageSender.CloseAsync();
            await messageSessionClient.CloseAsync();
        }
        static async Task SendMessagesWithSessionAsync(int numberOfMessagesToSend, int numberOfSessions)
        {
            try
            {
                for (var j = 0; j < numberOfSessions; j++)
                {
                    for (var i = 0; i < numberOfMessagesToSend; i++)
                    {
                        // Create a new message to send to the queue
                        string messageBody = $"Message {i}";
                        var message = new Message(Encoding.UTF8.GetBytes(messageBody));
                        message.SessionId = $"{j}";

                        // Write the body of the message to the console
                        Console.WriteLine($"Sending message: {messageBody} with sessionId {j}");

                        // Send the message to the queue
                        await messageSender.SendAsync(message);
                    }
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        static async Task ReceiveMessagesWithSessionAsync(int numberOfMessagesToReceiveInBatch, string sessionId)
        {
            var messageSession = await messageSessionClient.AcceptMessageSessionAsync();
            var messages = await messageSession.ReceiveAsync(numberOfMessagesToReceiveInBatch);
            if (messages == null)
            {
                Console.WriteLine($"Unable to get messages from sessionID {sessionId}");;
            }
            while(messages != null && messages.Count > 0)
            {
                // Process the message
                Console.WriteLine($"----------------------------------------------------");
                Console.WriteLine($"Received {messages.Count} from sessionID {sessionId}");
                Console.WriteLine($"----------------------------------------------------");
                foreach (Message message in messages)
                {
                    // Process the message
                    Console.WriteLine($"Received message: SessionId: {message.SessionId} SequenceNumber:{message.SystemProperties.SequenceNumber} Body:{Encoding.UTF8.GetString(message.Body)}");
                    // Complete the message so that it is not received again
                    await messageSession.CompleteAsync(message.SystemProperties.LockToken);
                }
                messages = await messageSession.ReceiveAsync(numberOfMessagesToReceiveInBatch);
            }
        }
    }
}
