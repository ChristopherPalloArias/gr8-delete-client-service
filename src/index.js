import express from 'express';
import cors from 'cors';
import amqp from 'amqplib';
import swaggerUi from 'swagger-ui-express';
import swaggerJsDoc from 'swagger-jsdoc';
import AWS from 'aws-sdk';

// AWS region and Lambda function configuration
const region = "us-east-2";
const lambdaFunctionName = "fetchSecretsFunction_gr8";

// Function to invoke Lambda y obtener los secretos
async function getSecretFromLambda() {
  const lambda = new AWS.Lambda({ region: region });
  const params = {
    FunctionName: lambdaFunctionName,
  };

  try {
    const response = await lambda.invoke(params).promise();
    const payload = JSON.parse(response.Payload);
    if (payload.errorMessage) {
      throw new Error(payload.errorMessage);
    }
    const body = JSON.parse(payload.body);
    return JSON.parse(body.secret);
  } catch (error) {
    console.error('Error invoking Lambda function:', error);
    throw error;
  }
}

// Función para iniciar el servicio
async function startService() {
  let secrets;
  try {
    secrets = await getSecretFromLambda();
  } catch (error) {
    console.error(`Error starting service: ${error}`);
    return;
  }

  AWS.config.update({
    region: region,
    accessKeyId: secrets.AWS_ACCESS_KEY_ID,
    secretAccessKey: secrets.AWS_SECRET_ACCESS_KEY,
  });

  const dynamoDB = new AWS.DynamoDB.DocumentClient();
  const app = express();
  const port = 8097;

  app.use(cors());
  app.use(express.json());

  // Configuración de Swagger
  const swaggerOptions = {
    swaggerDefinition: {
      openapi: '3.0.0',
      info: {
        title: 'Delete Client Service API',
        version: '1.0.0',
        description: 'API for deleting clients'
      }
    },
    apis: ['./src/index.js']
  };

  const swaggerDocs = swaggerJsDoc(swaggerOptions);
  app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocs));

  // Configuración de RabbitMQ
  let channel;
  async function connectRabbitMQ() {
    try {
      const connection = await amqp.connect('amqp://3.136.72.14:5672/');
      channel = await connection.createChannel();
      await channel.assertQueue('client-events', { durable: true });
      console.log('Connected to RabbitMQ');
    } catch (error) {
      console.error('Error connecting to RabbitMQ:', error);
    }
  }

  // Publicar evento en RabbitMQ
  const publishEvent = async (eventType, data) => {
    const event = { eventType, data };
    try {
      if (channel) {
        channel.sendToQueue('client-events', Buffer.from(JSON.stringify(event)), { persistent: true });
        console.log('Event published to RabbitMQ:', event);
      } else {
        console.error('Channel is not initialized');
      }
    } catch (error) {
      console.error('Error publishing event to RabbitMQ:', error);
    }
  };

  await connectRabbitMQ();

  /**
   * @swagger
   * /clients/{ci}:
   *   delete:
   *     summary: Delete a client
   *     description: Delete a client by ci
   *     parameters:
   *       - in: path
   *         name: ci
   *         required: true
   *         description: CI of the client to delete
   *         schema:
   *           type: string
   *     responses:
   *       200:
   *         description: Client deleted
   *         content:
   *           application/json:
   *             schema:
   *               type: object
   *               properties:
   *                 message:
   *                   type: string
   *                   example: "Client deleted"
   *       500:
   *         description: Error deleting client
   */
  app.delete('/clients/:ci', async (req, res) => {
    const { ci } = req.params;
    const tables = ['Clients_gr8', 'ClientsUpdate_gr8', 'ClientsList_gr8', 'ClientsDelete_gr8'];

    try {
      for (const table of tables) {
        const params = {
          TableName: table,
          Key: { ci }
        };
        await dynamoDB.delete(params).promise();
        console.log(`Client deleted from table: ${table}`);
      }

      // Publicar evento de eliminación de cliente en RabbitMQ
      publishEvent('ClientDeleted', { ci });

      res.send({ message: 'Client deleted' });
    } catch (error) {
      console.error('Error deleting client:', error);
      res.status(500).send({ message: 'Error deleting client', error });
    }
  });

  app.get('/', (req, res) => {
    res.send('Delete Client Service Running');
  });

  app.listen(port, () => {
    console.log(`Delete Client service listening at http://localhost:${port}`);
  });
}

startService();
