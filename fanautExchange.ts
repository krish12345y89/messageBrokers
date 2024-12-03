import amqplib, { Connection, Channel, ConsumeMessage } from "amqplib";

interface FanoutConfig {
    uri: string;
    exchangeName: string;
}

export class FanoutExchange {
    private connection: Connection | null = null;
    private channel: Channel | null = null;
    private exchange: amqplib.Replies.AssertExchange | null = null;
    protected config: FanoutConfig;

    constructor(config: FanoutConfig) {
        this.config = config;
        this.initialise();
    }

    private async initialise() {
        try {
            this.connection = await amqplib.connect(this.config.uri);
            console.log("Connection established");
            this.channel = await this.connection.createChannel();
            console.log("Channel created");
            this.exchange = await this.channel.assertExchange(this.config.exchangeName, "fanout", { durable: true });
            console.log("Exchange asserted successfully");
        } catch (error) {
            console.error("Failed to initialize connection or channel:", error);
            process.exit(1);
        }
    }

    public async sendMessage(data: object) {
        try {
            if (!this.channel) {
                await this.initialise();
                if (!this.channel) {
                    throw new Error("failed to initaise the channel")
                }
            }

            await this.channel.publish(this.config.exchangeName, "", Buffer.from(JSON.stringify(data)), {
                persistent: true,
            });
            console.log("Message sent successfully to exchange");
        } catch (error) {
            console.error("Failed to send message:", error);
        }
    }

    public async receiveMessage(queueName: string) {
        try {
            if (!this.channel) {
                await this.initialise();
                if (!this.channel) {
                    throw new Error("failed to initaise the channel")
                }
            }

            await this.channel.assertQueue(queueName, { durable: true });
            console.log(`Queue '${queueName}' asserted successfully`);

            await this.channel.bindQueue(queueName, this.config.exchangeName, "");
            console.log(`Queue '${queueName}' bound to exchange '${this.config.exchangeName}'`);

            await this.channel.consume(queueName, (data: ConsumeMessage | null) => {
                if (data && this.channel) {
                    const consumed = JSON.parse(data.content.toString());
                    console.log("Data consumed successfully:", consumed);
                    this.channel.ack(data);
                }
            }, { noAck: false });
        } catch (error) {
            console.error("Failed to receive message:", error);
        }
    }

    public async close() {
        try {
            if (this.channel) {
                await this.channel.close();
                console.log("Channel successfully closed");
            }
            if (this.connection) {
                await this.connection.close();
                console.log("Connection successfully closed");
            }
        } catch (error) {
            console.error("Failed to close connection:", error);
        }
    }
}