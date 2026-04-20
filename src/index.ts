import { randomUUID } from "node:crypto";
import { DeleteMessageCommand, Message, ReceiveMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { PublishCommand, SNSClient } from "@aws-sdk/client-sns";

const AWS_REGION = process.env.AWS_REGION ?? "us-east-1";
const SQS_URL = process.env.SQS_URL;
const DYNAMO_TABLE = process.env.DYNAMO_TABLE ?? "boletines";
const SNS_TOPIC_ARN = process.env.SNS_TOPIC_ARN;
const MOSTRADOR_BASE_URL = process.env.MOSTRADOR_BASE_URL ?? "http://localhost:8081";

if (!SQS_URL) {
  throw new Error("Falta configurar SQS_URL.");
}

const sqs = new SQSClient({ region: AWS_REGION });
const ddbClient = DynamoDBDocumentClient.from(new DynamoDBClient({ region: AWS_REGION }));
const sns = new SNSClient({ region: AWS_REGION });

function parsearMensaje(msg: Message) {
  if (!msg.Body) {
    throw new Error("Mensaje sin cuerpo.");
  }

  const parsed = JSON.parse(msg.Body);
  const contenido = String(parsed?.contenido ?? "").trim();
  const correoElectronico = String(parsed?.correoElectronico ?? parsed?.correoElectrónico ?? "").trim();
  const archivoUrl = String(parsed?.archivoUrl ?? "").trim();

  if (!contenido || !correoElectronico || !archivoUrl) {
    throw new Error("Mensaje inválido: faltan campos requeridos.");
  }

  return { contenido, correoElectronico, archivoUrl };
}

async function procesarMensaje(msg: Message) {
  const { contenido, correoElectronico, archivoUrl } = parsearMensaje(msg);
  const boletinID = randomUUID();

  await ddbClient.send(
    new PutCommand({
      TableName: DYNAMO_TABLE,
      Item: {
        boletinID,
        contenido,
        correoElectronico,
        archivoUrl,
        leido: false,
        creadoEn: new Date().toISOString(),
      },
    })
  );

  if (SNS_TOPIC_ARN) {
    const link = `${MOSTRADOR_BASE_URL}/boletines/${boletinID}?correoElectronico=${encodeURIComponent(correoElectronico)}`;
    await sns.send(
      new PublishCommand({
        TopicArn: SNS_TOPIC_ARN,
        Subject: "Nuevo boletín disponible",
        Message: `Se ha generado un nuevo boletín. Consulta en: ${link}`,
      })
    );
  }

  if (msg.ReceiptHandle) {
    await sqs.send(
      new DeleteMessageCommand({
        QueueUrl: SQS_URL,
        ReceiptHandle: msg.ReceiptHandle,
      })
    );
  }

  console.log(`Boletín procesado: ${boletinID}`);
}

async function consumir() {
  while (true) {
    try {
      const response = await sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: SQS_URL,
          MaxNumberOfMessages: 5,
          WaitTimeSeconds: 20,
          VisibilityTimeout: 60,
        })
      );

      const messages = response.Messages ?? [];
      for (const msg of messages) {
        try {
          await procesarMensaje(msg);
        } catch (error: any) {
          console.error("Error procesando mensaje:", error?.message ?? error);
        }
      }
    } catch (error: any) {
      console.error("Error consumiendo la cola:", error?.message ?? error);
    }
  }
}

console.log("Servicio procesador listo");

void consumir().catch((error: any) => {
  console.error("Fallo fatal del procesador:", error?.message ?? error);
  process.exit(1);
});