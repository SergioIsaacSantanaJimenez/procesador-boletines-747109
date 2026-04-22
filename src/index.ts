import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { DeleteMessageCommand, Message, ReceiveMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import { PublishCommand, SNSClient } from "@aws-sdk/client-sns";

const AWS_REGION = process.env.AWS_REGION ?? "us-east-1";
const SQS_URL = process.env.SQS_URL;
const DYNAMO_TABLE = process.env.DYNAMO_TABLE ?? "boletines";
const SNS_TOPIC_ARN = process.env.SNS_TOPIC_ARN;
const MOSTRADOR_BASE_URL = process.env.MOSTRADOR_BASE_URL ?? "http://localhost:8081";
const HEALTH_PORT = Number(process.env.HEALTH_PORT ?? 8082);

const runtime = {
  startedAt: new Date().toISOString(),
  lastLoopAt: "",
  lastProcessedAt: "",
  lastError: "",
};
let shuttingDown = false;

function log(level: "INFO" | "ERROR", message: string, meta?: Record<string, unknown>) {
  console.log(
    JSON.stringify({
      level,
      service: "procesador",
      message,
      timestamp: new Date().toISOString(),
      ...(meta ?? {}),
    })
  );
}

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

  runtime.lastProcessedAt = new Date().toISOString();
  runtime.lastError = "";
  log("INFO", "Boletin procesado", { boletinID });
}

async function consumir() {
  while (!shuttingDown) {
    try {
      runtime.lastLoopAt = new Date().toISOString();
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
          runtime.lastError = String(error?.message ?? error);
          log("ERROR", "Error procesando mensaje", { error: String(error?.message ?? error) });
        }
      }
    } catch (error: any) {
      runtime.lastError = String(error?.message ?? error);
      log("ERROR", "Error consumiendo cola", { error: String(error?.message ?? error) });
    }
  }

  log("INFO", "Bucle de consumo detenido");
}

const healthServer = createServer((req, res) => {
  if (req.url !== "/health") {
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: false, error: "Not Found" }));
    return;
  }

  const payload = {
    ok: true,
    service: "procesador",
    uptimeSec: Math.round(process.uptime()),
    startedAt: runtime.startedAt,
    lastLoopAt: runtime.lastLoopAt,
    lastProcessedAt: runtime.lastProcessedAt,
    lastError: runtime.lastError,
  };

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(payload));
}).listen(HEALTH_PORT, () => {
  log("INFO", "Health endpoint del procesador listo", { port: HEALTH_PORT });
});

log("INFO", "Servicio procesador listo");

function shutdown(signal: string) {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  log("INFO", "Recibida senal de apagado", { signal });

  healthServer.close((error?: Error) => {
    if (error) {
      log("ERROR", "Error cerrando health server", { error: String(error.message) });
    }
  });

  setTimeout(() => {
    log("INFO", "Apagado completado");
    process.exit(0);
  }, 1000).unref();

  setTimeout(() => {
    log("ERROR", "Cierre forzado por timeout");
    process.exit(1);
  }, 25000).unref();
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

void consumir().catch((error: any) => {
  log("ERROR", "Fallo fatal del procesador", { error: String(error?.message ?? error) });
  process.exit(1);
});