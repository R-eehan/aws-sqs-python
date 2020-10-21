'''
Boto3 documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html

Remember: 'sudo pip install boto3' maybe required on Amazon Linux AMI. If 'Run' doesn't work on the Cloud9 enviroment, run this file manually. 
'''

# Load the AWS SDK for Python
import boto3, time

# Load the exceptions for error handling
from botocore.exceptions import ClientError, ParamValidationError

# Create AWS service client and set region
sqs = boto3.client('sqs', region_name='us-east-1')

# Create an SQS queue
def create_sqs_queue(sqs_queue_name):
    try:
        data = sqs.create_queue(
            QueueName = sqs_queue_name,
            Attributes = {
                'ReceiveMessageWaitTimeSeconds': '20',
                'VisibilityTimeout': '60'
            }
        )
        return data['QueueUrl']
    # An error occurred
    except ParamValidationError as e:
        print("Parameter validation error: %s" % e)
    except ClientError as e:
        print("Client error: %s" % e)

#Sending 50 SQS queue messages
def create_messages(queue_url):
    #Creating 50 messages
    TempMessages = []
    for a in range(50):
        tempStr = 'This is the content for message ' + str(a)
        TempMessages.append(tempStr)
    
    #Delivering the messages to the SQS queue_url
    for message in TempMessages:
        try:
            data = sqs.send_message(
                QueueUrl = queue_url,
                MessageBody = message
                )
            print(data['MessageId'])
        
        except ParamValidationError as e:
            print(f"ParamValidationError: {e}")
        except ClientError as e:
            print(f"ClientError: {e}")

#For creating messages in SQS via SNS
sns = boto3.client('sns', region_name='us-east-1')
def create_messages():
    try:
        data = sns.publish(
            TargetARN = 'arn:aws:sns:us-east-1:801868729718:backspace-lab',  #TargetARN is the SNS topic we're sending a message to. AWS SNS will in turn publish this message to its subscribers, which in this case is our SQS queue.
            Subject = 'SNS Message 2',
            Message = 'This is another message from AWS SNS!'
            )
        return data['MessageId']
    except ParamValidationError as e:
        print(f"ParamValidationError: {e}")
    except ClientError as e:
        print(f"ClientError: {e}")
            
#Create 50 SQS messages in batches
def create_messages_in_batches(queue_url):
    TempMessages = []
    for a in range(5):
        TempEntries = []
        for b in range(10):
            tempStr1 = 'This is the content for message ' + str((a*10+b))
            tempStr2 = 'Message' + str((a*10+b))
            tempEntry = {
                'MessageBody': tempStr1,
                'Id': tempStr2
            }
            TempEntries.append(tempEntry)
        TempMessages.append(TempEntries)
        print(TempMessages)
        
    # Deliver messages to SQS queue_url
    for batch in TempMessages:
        try:
            data = sqs.send_message_batch(
                QueueUrl = queue_url,
                Entries = batch
            )
            print(data['Successful'])
        # An error occurred
        except ParamValidationError as e:
            print(f"Parameter validation error: {e}")
        except ClientError as e:
            print("Client error: {e}")
            
#Receiving SQS messages from the queue and then deleting the messages            
def receive_messages(queue_url):
    print("Reading messages in the queue")
    while True:
        try:
            data = sqs.receive_message(
                QueueUrl = queue_url,
                VisibilityTimeout = 60,
                MaxNumberOfMessages = 10,
                WaitTimeSeconds = 20
            )
        except ParamValidationError as e:
            print(f"ParamValidationError: {e}")
        except ClientError as e:
            print(f"ClientError: {e}")
            
        try:
            data["Messages"]
        except KeyError:
            data = None
            
        if data is None:
            print("Queue is empty, waiting for 60 seconds")
            time.sleep(60)
        else:
            for message in data["Messages"]:
                print(message)
                sqs.delete_message(
                    QueueUrl = queue_url,
                    ReceiptHandle = message["ReceiptHandle"]
                )
                print("Deleted Message")
                time.sleep(1)

# Main program
def main():
    sqs_queue_url = create_sqs_queue('backspace-lab')
    print('Successfully created SQS queue URL '+ sqs_queue_url )

    # create_messages_in_batches(sqs_queue_url)
    # print("Successfully created 50 messages in batches!")

    create_messages()
    print("Successfully published a message from SNS!")
    
    receive_messages(sqs_queue_url)
    print("Received")

if __name__ == '__main__':
    main()
    
