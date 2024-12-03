import amqplib, { Connection, Channel } from "amqplib";
import { v4 as uuid } from "uuid";

interface RpcConfig {
    uri: string;
}

export class Rpc {
    protected config: RpcConfig;
    private connection: Connection | null = null;
    private channel: Channel | null = null;

    constructor(config: RpcConfig) {
        this.config = config;
        this.initialise();
    }

    private async initialise() {
        try {
            this.connection = await amqplib.connect(this.config.uri);
            console.log("Connected to the RabbitMQ server successfully.");
            this.channel = await this.connection.createChannel();
            console.log("Channel created successfully.");
        } catch (error) {
            console.error("Initialization of connection and channel failed due to:", error);
            process.exit(1);
        }
    }

    public async requestData(queueName: string, data: object) {
        try {
            if (!this.channel) {
                await this.initialise();
                if (this.channel==null) {
                    throw new Error("failed to initaise the channel");
                }
            }

            const correlationalId: string = uuid();
            const q = await this.channel.assertQueue("", { exclusive: true });
            console.log(`Temporary queue '${q.queue}' asserted successfully.`);
            await this.channel.assertQueue(queueName, { durable: true });
            console.log(`Queue '${queueName}' asserted successfully.`);

            const sendedData = Buffer.from(JSON.stringify(data));
            await this.channel.sendToQueue(queueName, sendedData, {
                replyTo: q.queue,
                persistent: true,
                correlationId: correlationalId,
            });

            return new Promise((res, rej) => {
                const timeout = setTimeout(() => {
                    rej("Failed to retrieve the data: Timeout");
                }, 20000);

                this.channel!.consume(q.queue, (msg) => {
                    if (msg && msg.properties.correlationId === correlationalId) {
                        clearTimeout(timeout);
                        const consumed = JSON.parse(msg.content.toString());
                        console.log({ sendedData: sendedData.toString(), receivedData: consumed });
                        this.channel!.ack(msg);
                        res(consumed);
                    }
                }, { noAck: false });
            });
        } catch (error) {
            console.error("Failed to request the data due to:", error);
        }
    }

    public async sendData(queueName: string, fakeResponse: object) {
        try {
            if (this.channel == null) {
                throw new Error("Failed to get the channel");
            }

            const sendedData = Buffer.from(JSON.stringify(fakeResponse));
            await this.channel.assertQueue(queueName, { durable: true });
            console.log(`Queue '${queueName}' asserted successfully for sending data.`);

            await this.channel.consume(queueName, async (data) => {
                if (data && data.content && this.channel) {
                    const consumed = JSON.parse(data.content.toString());
                    const replyTo = data.properties.replyTo;
                    const correlationId = data.properties.correlationId;

                    console.log({ consumedData: consumed });
                    this.channel.ack(data);
                    await this.channel.sendToQueue(replyTo, sendedData, { correlationId: correlationId, persistent: true });
                    console.log("Data sent successfully.");
                } else {
                    console.error("Failed to retrieve data from the queue.");
                }
            }, { noAck: false });
 } catch (error) {
            console.error("Failed to send the data due to:", error);
        }
    }
}
