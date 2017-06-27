using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Amazon.Auth.AccessControlPolicy;
using Amazon.Auth.AccessControlPolicy.ActionIdentifiers;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace AwsSnsAndSqs
{
    class Program
    {
        public static void Main(string[] args)
        {
            const bool useEasySubscription = false;
            var sns = new AmazonSimpleNotificationServiceClient();
            var sqs = new AmazonSQSClient();

            string nameOfNewTopic = args[0];  //Sanitise this to ensure no illegal characters.
            var emailAddress = args[1];

            try
            {
                var topicArn = sns.CreateTopic(
                    new CreateTopicRequest { Name = nameOfNewTopic }).TopicArn;

                sns.SetTopicAttributes(new SetTopicAttributesRequest
                {
                    TopicArn = topicArn,
                    AttributeName = "DisplayName",
                    AttributeValue = "Sample Notifications"
                });

                RetrieveAllTopics(sns);

                if (string.IsNullOrEmpty(emailAddress) == false)
                {
                    // Subscribe an endpoint - in this case, an email address
                    Console.WriteLine();
                    Console.WriteLine("Subscribing email address {0} to topic...", emailAddress);
                    sns.Subscribe(new SubscribeRequest
                    {
                        TopicArn = topicArn,
                        Protocol = "email",
                        Endpoint = emailAddress
                    });

                    // When using email, recipient must confirm subscription
                    Console.WriteLine();
                    Console.WriteLine("Please check your email and press enter when you are subscribed...");
                    Console.ReadLine();                    
                }

                Console.WriteLine();
                var sqsRequest = new CreateQueueRequest
                {
                    QueueName = "MyExperimentQueue"
                };

                var createQueueResponse = sqs.CreateQueue(sqsRequest);
                var myQueueUrl = createQueueResponse.QueueUrl;

                var myQueueArn = sqs.GetQueueAttributes(
                    new GetQueueAttributesRequest
                    {
                        QueueUrl = myQueueUrl,
                        AttributeNames = new List<string> { "All"}
                    }).QueueARN;

                ListQueues(sqs);

                if (myQueueArn != null)
                {
                    //https://aws.amazon.com/blogs/developer/subscribing-an-sqs-queue-to-an-sns-topic/

                    if (useEasySubscription)
                    {
                        sns.SubscribeQueue(topicArn, sqs, myQueueUrl);
                    }
                    else
                    {
                        var subscribeRequest = new SubscribeRequest(topicArn, "SQS", myQueueArn);

                        sns.Subscribe(subscribeRequest);

                        ActionIdentifier[] actions = new ActionIdentifier[2];
                        actions[0] = SQSActionIdentifiers.SendMessage;
                        actions[1] = SQSActionIdentifiers.ReceiveMessage;

                        Policy sqsPolicy = new Policy()
                            .WithStatements(new Statement(Statement.StatementEffect.Allow)
                                            .WithPrincipals(Principal.AllUsers)
                                            .WithResources(new Resource(myQueueArn))
                                            .WithConditions(ConditionFactory.NewSourceArnCondition(topicArn))
                                             .WithActionIdentifiers(actions));


                        var attributeDictionary = new Dictionary<string, string>();
                        attributeDictionary.Add("Policy", sqsPolicy.ToJson());
                        var attributes = new SetQueueAttributesRequest { QueueUrl = myQueueUrl, Attributes = attributeDictionary };

                        sqs.SetQueueAttributes(attributes);
                    }

                    Thread.Sleep(TimeSpan.FromSeconds(5));

                    // Publish message
                    Console.WriteLine();
                    Console.WriteLine("Publishing message to topic...");
                    sns.Publish(new PublishRequest
                    {
                        Subject = "Test",
                        Message = "Testing testing 1 2 3",
                        TopicArn = topicArn
                    });   
                    var receivedMessageResponse = ReceiveMessage(sqs, myQueueUrl);

                    DeleteReceivedMessage(receivedMessageResponse, myQueueUrl, sqs);                                     
                }

                //Console.WriteLine();
                //Console.WriteLine("Deleting topic...");
                //sns.DeleteTopic(new DeleteTopicRequest
                //{
                //    TopicArn = topicArn
                //});
            }
            catch (AmazonSimpleNotificationServiceException ex)
            {
                Console.WriteLine("Caught Exception: " + ex.Message);
                Console.WriteLine("Response Status Code: " + ex.StatusCode);
                Console.WriteLine("Error Code: " + ex.ErrorCode);
                Console.WriteLine("Error Type: " + ex.ErrorType);
                Console.WriteLine("Request ID: " + ex.RequestId);
            }

            Console.WriteLine();
            Console.WriteLine("Press enter to exit...");
            Console.ReadLine();
        }


        private static void ListQueues(IAmazonSQS sqs)
        {
            var listQueuesRequest = new ListQueuesRequest();
            var listQueuesResponse = sqs.ListQueues(listQueuesRequest);

            Console.WriteLine("Printing list of Amazon SQS queues.\n");
            if (listQueuesResponse.QueueUrls != null)
            {
                foreach (var queueUrl in listQueuesResponse.QueueUrls)
                {
                    Console.WriteLine("  QueueUrl: {0}", queueUrl);
                }
            }
            Console.WriteLine();
        }


        private static void DeleteReceivedMessage(ReceiveMessageResponse receiveMessageResponse, string myQueueUrl, IAmazonSQS sqs)
        {
            if (receiveMessageResponse.Messages.Any())
            {
                var messageRecieptHandle = receiveMessageResponse.Messages[0].ReceiptHandle;

                //Deleting a message
                Console.WriteLine("Deleting the message.\n");
                var deleteRequest = new DeleteMessageRequest { QueueUrl = myQueueUrl, ReceiptHandle = messageRecieptHandle };
                sqs.DeleteMessage(deleteRequest);                
            }
        }


        private static ReceiveMessageResponse ReceiveMessage(IAmazonSQS sqs, string myQueueUrl)
        {
            var receiveMessageRequest = new ReceiveMessageRequest
            {
                QueueUrl = myQueueUrl,
                WaitTimeSeconds = 20
            };
            var receiveMessageResponse = sqs.ReceiveMessage(receiveMessageRequest);

            if (receiveMessageResponse.Messages != null)
            {
                Console.WriteLine("Printing received message.\n");
                foreach (var message in receiveMessageResponse.Messages)
                {
                    Console.WriteLine("  Message");
                    if (!string.IsNullOrEmpty(message.MessageId))
                    {
                        Console.WriteLine("    MessageId: {0}", message.MessageId);
                    }
                    if (!string.IsNullOrEmpty(message.ReceiptHandle))
                    {
                        Console.WriteLine("    ReceiptHandle: {0}", message.ReceiptHandle);
                    }
                    if (!string.IsNullOrEmpty(message.MD5OfBody))
                    {
                        Console.WriteLine("    MD5OfBody: {0}", message.MD5OfBody);
                    }
                    if (!string.IsNullOrEmpty(message.Body))
                    {
                        Console.WriteLine("    Body: {0}", message.Body);
                    }

                    foreach (var attributeKey in message.Attributes.Keys)
                    {
                        Console.WriteLine("  Attribute");
                        Console.WriteLine("    Name: {0}", attributeKey);
                        var value = message.Attributes[attributeKey];
                        Console.WriteLine("    Value: {0}", string.IsNullOrEmpty(value) ? "(no value)" : value);
                    }
                }
            }

            return receiveMessageResponse;
        }


        private static void RetrieveAllTopics(AmazonSimpleNotificationServiceClient sns)
        {
            Console.WriteLine("Retrieving all topics...");
            var listTopicsRequest = new ListTopicsRequest();

            ListTopicsResponse listTopicsResponse;
            do
            {
                listTopicsResponse = sns.ListTopics(listTopicsRequest);
                foreach (var topic in listTopicsResponse.Topics)
                {
                    Console.WriteLine(" Topic: {0}", topic.TopicArn);

                    // Get topic attributes
                    var topicAttributes = sns.GetTopicAttributes(new GetTopicAttributesRequest
                    {
                        TopicArn = topic.TopicArn
                    }).Attributes;

                    if (topicAttributes.Count > 0)
                    {
                        Console.WriteLine(" Topic attributes");
                        foreach (var topicAttribute in topicAttributes)
                        {
                            Console.WriteLine(" -{0} : {1}", topicAttribute.Key, topicAttribute.Value);
                        }
                    }
                    Console.WriteLine();
                }
                listTopicsRequest.NextToken = listTopicsResponse.NextToken;
            } while (listTopicsResponse.NextToken != null);
        }

    }
}