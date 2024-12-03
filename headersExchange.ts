import amqplib, { Connection, Channel, ConsumeMessage } from "amqplib";

interface HeadersConfig {
    uri: string;
    exchangeName: string;
}

class HeadersExchange {
    private connection: Connection | null = null;
    private channel: Channel | null = null;
    protected config: HeadersConfig;
    private exchange: amqplib.Replies.AssertExchange | null = null;

    constructor(config: HeadersConfig) {
        this.config = config;
        this.initialise();
    }

    private async initialise() {
        try {
            this.connection = await amqplib.connect(this.config.uri);
            this.channel = await this.connection.createChannel();
            this.exchange = await this.channel.assertExchange(this.config.exchangeName, "headers", { durable: true });
            console.log(`Headers exchange '${this.config.exchangeName}' initialized successfully.`);
        } catch (error) {
            console.error("Failed to initialize the connection or channel:", error);
            process.exit(1);
        }
    }

    public async sendMessage(data: object, headers: object) {
        try {
            if (!this.channel) {
                await this.initialise();
                if (!this.channel) {
                    throw new Error("failed to initaise the channel")
                }
            }

            if (!this.exchange) {
                throw new Error("Exchange is not initialized.");
            }

            const messageBuffer = Buffer.from(JSON.stringify(data));
            await this.channel.publish(this.config.exchangeName, "", messageBuffer, {
                headers: headers,
                persistent: true,
            });
            console.log("Message sent successfully:", data, "with headers:", headers);
        } catch (error) {
            console.error("Failed to send message:", error);
        }
    }

    public async receiveMessage(headers: object): Promise<void> {
        try {
            if (!this.channel) {
                await this.initialise();
                if (!this.channel) {
                    throw new Error("failed to initaise the channel")
                }
            }

            if (!this.exchange) {
                throw new Error("Exchange is not initialized.");
            }

            const q = await this.channel.assertQueue("", {
                durable: true,
                exclusive: true,
            });

            await this.channel.bindQueue(q.queue, this.config.exchangeName, "", { headers: headers });
            console.log(`Waiting for messages in queue '${q.queue}'...`);

            await this.channel.consume(q.queue, (data: ConsumeMessage | null) => {
                if (data && this.channel) {
                    const consumedMessage = JSON.parse(data.content.toString());
                    console.log("Message received successfully:", consumedMessage);
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
                console.log("Channel successfully closed.");
            }
            if (this.connection) {
                await this.connection.close();
                console.log("Connection successfully closed.");
            }
        } catch (error) {
            console.error("Failed to close the connection:", error);
        }
    }
}