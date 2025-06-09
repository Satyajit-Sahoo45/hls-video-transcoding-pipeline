import {
  SQSClient,
  ReceiveMessageCommand,
  ReceiveMessageCommandOutput,
  DeleteMessageCommand,
} from "@aws-sdk/client-sqs";
import {
  AssignPublicIp,
  ECSClient,
  LaunchType,
  RunTaskCommand,
} from "@aws-sdk/client-ecs";

const sqsClient = new SQSClient({
  region: "us-east-1",
  credentials: {
    accessKeyId: "",
    secretAccessKey: "",
  },
});
const ecsClient = new ECSClient({
  region: "us-east-1",
  credentials: {
    accessKeyId: "",
    secretAccessKey: "",
  },
});
const queueUrl =
  "https://sqs.us-east-1.amazonaws.com/933723420264/transcode-queue";

async function init() {
  const params = {
    QueueUrl: queueUrl,
    MaxNumberOfMessages: 1,
    WaitTimeSeconds: 10,
  };

  const command: ReceiveMessageCommand = new ReceiveMessageCommand(params);

  while (true) {
    try {
      console.log("Polling ...");
      const { Messages }: ReceiveMessageCommandOutput = await sqsClient.send(
        command
      );

      if (!Messages) {
        console.log(`No message in Queue`);
        continue;
      }

      for (const message of Messages) {
        try {
          const { MessageId, Body } = message;
          if (!Body) {
            console.log("Received message with empty body, skipping");
            continue;
          }

          // validate and parse the event
          const event = JSON.parse(Body);
          if ("Service" in event && "Event" in event) {
            if (event.Event === "s3:TestEvent") {
              console.log("Ignoring test event");
              continue;
            }
          }
          console.log(`Message Received`, { MessageId, Body });

          // 1. create a repo in the AWS-ECR and push the docker container for the transcoder
          // 2. then create a cluster in ECS service (keep the infrastructure as FARGATE)
          // 3. then create a task in ECS task defination section and keep lunch type AWS FARGATE, docker-image URI in container details

          // spin the docker container
          for (const record of event.Records) {
            const { s3 } = record;

            const {
              bucket,
              object: { key },
            } = s3;

            // linking docker container with consumer to run the transcoder container through the consumer after receving events
            const input = {
              cluster: "arn:aws:ecs:us-east-1:654443420264:cluster/hls-dev", // created cluster's ARN. (to specify a task to a cluster)
              taskDefinition:
                "arn:aws:ecs:us-east-1:654443420264:task-definition/video-transcoder:1", // created task's ARN
              launchType: "FARGATE" as LaunchType | undefined,
              networkConfiguration: {
                awsvpcConfiguration: {
                  assignPublicIp: "ENABLED" as AssignPublicIp | undefined,
                  securityGroups: ["sg-011b08bbbf713a0fb"],
                  subnets: [
                    "subnet-09e8e921492d48851",
                    "subnet-0784c4ce41f752a86",
                    "subnet-01f31b732a9243baa",
                  ],
                },
              },
              overrides: {
                containerOverrides: [
                  {
                    name: "video-transcoder", // name of the created container in ECS task
                    environment: [
                      { name: "BUCKET_NAME", value: bucket.name }, // original video's upload container name
                      { name: "KEY", value: key },
                    ],
                  },
                ],
              },
            };

            const runTaskCommand = new RunTaskCommand(input);

            try {
              await ecsClient.send(runTaskCommand);
              console.log(`ECS task launched for ${bucket.name}/${key}`);
            } catch (error) {
              console.error(
                `Failed to launch ECS task for ${bucket.name}/${key}:`,
                error
              );
              // retry mechanism here
              continue;
            }
          }

          // Delete the message from queue
          try {
            await sqsClient.send(
              new DeleteMessageCommand({
                QueueUrl: queueUrl,
                ReceiptHandle: message.ReceiptHandle,
              })
            );
            console.log(`Message ${MessageId} deleted from queue`);
          } catch (error) {
            console.error(
              `Failed to delete message ${MessageId} from queue:`,
              error
            );
            // retry mechanism for message deletion
          }
        } catch (messageError) {
          console.error("Error processing message:", messageError);
          // Log the problematic message
          console.error("Problematic message:", message);
        }
      }
    } catch (pollingError) {
      console.error("Error during polling:", pollingError);
      // Implement a delay before retrying to avoid hammering the service
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
}

init().catch((error) => {
  console.error("Fatal error in init function:", error);
  process.exit(1);
});
