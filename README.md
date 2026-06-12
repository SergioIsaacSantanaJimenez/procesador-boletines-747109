# Procesador de Boletines

Este proyecto implementa un microservicio encargado de procesar mensajes entrantes de una cola de mensajería (AWS SQS), persistir la información relevante en una base de datos NoSQL (AWS DynamoDB) y, opcionalmente, notificar a través de un servicio de publicación/suscripción (AWS SNS) la disponibilidad de un nuevo boletín. Diseñado para entornos distribuidos, opera en un bucle continuo de consumo de mensajes, asegurando el procesamiento y la persistencia de datos de manera robusta.

## Características

*   **Consumo de Mensajes:** Procesa mensajes de una cola SQS de AWS de forma continua.
*   **Procesamiento de Datos:** Extrae `contenido`, `correoElectronico` y `archivoUrl` de los mensajes entrantes.
*   **Persistencia:** Almacena los datos procesados en una tabla DynamoDB, incluyendo un ID único generado (`boletinID`) y metadatos de creación.
*   **Notificación:** Publica un mensaje en un topic SNS configurado, incluyendo un enlace dinámico al boletín (asumiendo un servicio Mostrador para la visualización).
*   **Eliminación de Mensajes:** Elimina mensajes de la cola SQS solo después de su procesamiento exitoso.
*   **Manejo de Errores:** Incluye logging estructurado para el monitoreo de errores y eventos importantes.
*   **Cierre Controlado:** Implementa un mecanismo de "graceful shutdown" para un apagado seguro del servicio.
*   **Contenedorización:** Configuración para empaquetado y despliegue utilizando Docker.

## Tecnologías Utilizadas

*   **TypeScript**: `^5.8.3`
*   **Node.js**: `v20` (basado en la imagen Docker `node:20-alpine`)
*   **AWS SDK v3**: `^3.782.0`
    *   `@aws-sdk/client-dynamodb`
    *   `@aws-sdk/client-sns`
    *   `@aws-sdk/client-sqs`
    *   `@aws-sdk/lib-dynamodb`
*   **Docker**

## Prerrequisitos

*   **Node.js**: Versión 20 o superior.
*   **npm**: Gestor de paquetes de Node.js.
*   **Acceso a AWS**: Credenciales de AWS configuradas localmente o en el entorno de despliegue, con permisos para:
    *   `sqs:ReceiveMessage`, `sqs:DeleteMessage`
    *   `dynamodb:PutItem`
    *   `sns:Publish`
*   **Variables de Entorno**:
    *   `AWS_REGION`: Región de AWS donde se encuentran los servicios (ej. `us-east-1`).
    *   `SQS_URL`: URL completa de la cola SQS de donde se consumirán los mensajes.
    *   `DYNAMO_TABLE`: Nombre de la tabla DynamoDB donde se almacenarán los boletines (por defecto: `boletines`).
    *   `SNS_TOPIC_ARN`: ARN del topic SNS al que se enviarán las notificaciones (opcional).
    *   `MOSTRADOR_BASE_URL`: URL base del servicio que mostrará los boletines (por defecto: `http://localhost:8081`).

## Instalación

1.  Clona el repositorio:
    ```bash
    git clone https://github.com/SergioIsaacSantanaJimenez/procesador-boletines-747109.git
    cd procesador-boletines-747109
    ```
2.  Instala las dependencias:
    ```bash
    npm install
    ```

## Ejecución

### En entorno de desarrollo (local)

1.  Compila el código TypeScript a JavaScript:
    ```bash
    npm run build
    ```
2.  Establece las variables de entorno (ejemplo para Linux/macOS):
    ```bash
    export AWS_REGION="tu-region-aws"
    export SQS_URL="https://sqs.tu-region-aws.amazonaws.com/123456789012/nombre-tu-cola"
    export DYNAMO_TABLE="boletines-de-prueba"
    export SNS_TOPIC_ARN="arn:aws:sns:tu-region-aws:123456789012:nombre-tu-topic" # Opcional
    export MOSTRADOR_BASE_URL="http://localhost:8081" # Opcional, si Mostrador corre local
    ```
    *Para Windows (PowerShell):*
    ```powershell
    $env:AWS_REGION="tu-region-aws"
    $env:SQS_URL="https://sqs.tu-region-aws.amazonaws.com/123456789012/nombre-tu-cola"
    $env:DYNAMO_TABLE="boletines-de-prueba"
    $env:SNS_TOPIC_ARN="arn:aws:sns:tu-region-aws:123456789012:nombre-tu-topic"
    $env:MOSTRADOR_BASE_URL="http://localhost:8081"
    ```
3.  Inicia el servicio:
    ```bash
    npm start
    ```

### Con Docker

1.  Construye la imagen Docker:
    ```bash
    docker build -t procesador-boletines .
    ```
2.  Ejecuta el contenedor, pasando las variables de entorno necesarias:
    ```bash
    docker run -d \
      -e AWS_REGION="tu-region-aws" \
      -e SQS_URL="https://sqs.tu-region-aws.amazonaws.com/123456789012/nombre-tu-cola" \
      -e DYNAMO_TABLE="boletines-de-prueba" \
      -e SNS_TOPIC_ARN="arn:aws:sns:tu-region-aws:123456789012:nombre-tu-topic" \
      -e MOSTRADOR_BASE_URL="http://localhost:8081" \
      --name procesador-boletines-instancia \
      procesador-boletines
    ```

## Estructura del Proyecto

```
.
├── Dockerfile                  # Define la imagen Docker para la aplicación.
├── package.json                # Metadatos del proyecto y dependencias.
└── src/
    └── index.ts                # Lógica principal del procesador de boletines.
└── dist/                       # Directorio de salida para el código JavaScript compilado (generado al compilar).
```

## Habilidades Técnicas Demostradas

Este proyecto demuestra experiencia en:

*   **Desarrollo Backend con TypeScript y Node.js**: Construcción de lógica de negocio robusta y escalable.
*   **Integración con Servicios AWS**: Dominio de la interacción programática con AWS SQS para colas de mensajes, AWS DynamoDB para persistencia NoSQL y AWS SNS para notificaciones distribuidas.
*   **Arquitecturas Basadas en Eventos (EDA)**: Implementación de un consumidor de mensajes para procesar eventos de forma asíncrona.
*   **Manejo de Asincronía**: Uso efectivo de `async/await` para operaciones no bloqueantes.
*   **Gestión de Configuración**: Utilización de variables de entorno para una configuración flexible y segura del servicio.
*   **Logging y Observabilidad**: Implementación de logging estructurado para facilitar el monitoreo y la depuración.
*   **Resistencia y Fiabilidad**: Manejo de errores y mecanismos de reintento implícitos en SQS (visibilidad timeout), junto con un cierre controlado para minimizar la pérdida de datos y garantizar la estabilidad.
*   **Contenedorización con Docker**: Empaquetado de la aplicación en un contenedor para facilitar el despliegue y la portabilidad.
